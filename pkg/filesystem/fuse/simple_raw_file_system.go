// +build darwin linux

package fuse

import (
	"fmt"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type directoryHandle struct {
	directory Directory

	// Used by ReadDir().
	readDirLock        sync.Mutex
	readDirInitialized bool
	readDirEntries     dirEntryList

	// Used by ReadDirPlus().
	readDirPlusLock        sync.Mutex
	readDirPlusInitialized bool
	readDirPlusDirectories directoryDirEntryList
	readDirPlusLeaves      leafDirEntryList
}

type directoryEntry struct {
	directory Directory
	nLookup   uint64
}

type leafEntry struct {
	leaf    Leaf
	nLookup uint64
}

// Node is the intersection between Directory and Leaf. These are the
// operations that can be applied to both kinds of objects.
type node interface {
	FUSEAccess(mask uint32) fuse.Status
	FUSEGetAttr(out *fuse.Attr)
	FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status
}

type initializedServer struct {
	rawFileSystem *simpleRawFileSystem
	server        fuse.ServerCallbacks
}

// SimpleRawFileSystemServerCallbacks keeps track of a list of all
// ServerCallbacks objects provided by go-fuse. Calls to any of the
// notification functions will be forwarded to all registered callbacks.
type SimpleRawFileSystemServerCallbacks struct {
	lock               sync.RWMutex
	initializedServers []initializedServer
}

// EntryNotify can be called to report that directory entries have been
// removed. This causes them to be removed from the directory entry
// cache used by FUSE as well.
func (sc *SimpleRawFileSystemServerCallbacks) EntryNotify(parent uint64, name path.Component) fuse.Status {
	sc.lock.RLock()
	initializedServers := sc.initializedServers
	sc.lock.RUnlock()

	for _, initializedServer := range initializedServers {
		rfs := initializedServer.rawFileSystem
		if parent == rfs.rootDirectoryInodeNumber {
			// Even though we permit the root directory to
			// have an arbitrary inode number, FUSE requires
			// that the root directory uses node ID 1.
			if s := initializedServer.server.EntryNotify(fuse.FUSE_ROOT_ID, name.String()); s != fuse.OK {
				return s
			}
		} else {
			// Discard invalidations for directory entries
			// if the containing directory isn't known to
			// the kernel. These requests would fail anyway.
			rfs.nodeLock.RLock()
			_, ok := rfs.directories[parent]
			rfs.nodeLock.RUnlock()
			if ok {
				if s := initializedServer.server.EntryNotify(parent, name.String()); s != fuse.OK {
					return s
				}
			}
		}
	}
	return fuse.OK
}

type simpleRawFileSystem struct {
	rootDirectoryInodeNumber uint64
	directorySorter          Sorter
	serverCallbacks          *SimpleRawFileSystemServerCallbacks

	// Maps to resolve node IDs to directories and leaves.
	nodeLock    sync.RWMutex
	directories map[uint64]directoryEntry
	leaves      map[uint64]leafEntry

	// Map to resolve file handles to readdir() state.
	directoryHandlesLock sync.RWMutex
	nextDirectoryHandle  uint64
	directoryHandles     map[uint64]*directoryHandle
}

// NewSimpleRawFileSystem creates a go-fuse RawFileSystem that converts
// flat FUSE operations to calls against a hierarchy of Directory and
// Leaf objects.
//
// This implementation is comparable to the RawFileSystem
// implementations created using go-fuse's fs.NewNodeFS() and
// nodefs.FileSystemConnector.RawFS(), except that it is simpler. It
// does not contain an inode number allocator, nor does it attempt to
// keep track of files stored in a directory. Tracking this information
// is the responsibility of the Directory and Leaf implementations.
//
// FUSE as a protocol makes no true distinction between Directory and
// Leaf objects. This implementation could therefore have been
// simplified a bit by merging these two interface types together.
// Separation between these two interfaces was added to make it easier
// to understand which operations actually get called against a given
// object type.
func NewSimpleRawFileSystem(rootDirectory Directory, rootDirectoryInodeNumber uint64, directorySorter Sorter, serverCallbacks *SimpleRawFileSystemServerCallbacks) fuse.RawFileSystem {
	return &simpleRawFileSystem{
		rootDirectoryInodeNumber: rootDirectoryInodeNumber,
		directorySorter:          directorySorter,
		serverCallbacks:          serverCallbacks,

		directories: map[uint64]directoryEntry{
			fuse.FUSE_ROOT_ID: {
				directory: rootDirectory,
				nLookup:   1,
			},
		},
		leaves: map[uint64]leafEntry{},

		nextDirectoryHandle: 1,
		directoryHandles:    map[uint64]*directoryHandle{},
	}
}

func (rfs *simpleRawFileSystem) populateEntryOut(out *fuse.EntryOut) {
	out.NodeId = out.Ino
}

func (rfs *simpleRawFileSystem) getDirectoryLocked(nodeID uint64) Directory {
	if entry, ok := rfs.directories[nodeID]; ok {
		return entry.directory
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known directory", nodeID))
}

func (rfs *simpleRawFileSystem) getLeafLocked(nodeID uint64) Leaf {
	if entry, ok := rfs.leaves[nodeID]; ok {
		return entry.leaf
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known leaf", nodeID))
}

func (rfs *simpleRawFileSystem) getNodeLocked(nodeID uint64) node {
	if entry, ok := rfs.directories[nodeID]; ok {
		return entry.directory
	}
	if entry, ok := rfs.leaves[nodeID]; ok {
		return entry.leaf
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known directory or leaf", nodeID))
}

func (rfs *simpleRawFileSystem) addDirectory(i Directory, out *fuse.EntryOut) {
	rfs.populateEntryOut(out)

	rfs.nodeLock.Lock()
	defer rfs.nodeLock.Unlock()

	if _, ok := rfs.leaves[out.NodeId]; ok {
		panic(fmt.Sprintf("Directory %d has the same node ID as an existing leaf", out.NodeId))
	}

	// Increment lookup count of directory.
	rfs.directories[out.NodeId] = directoryEntry{
		directory: i,
		nLookup:   rfs.directories[out.NodeId].nLookup + 1,
	}
}

func (rfs *simpleRawFileSystem) addLeaf(i Leaf, out *fuse.EntryOut) {
	rfs.populateEntryOut(out)

	rfs.nodeLock.Lock()
	defer rfs.nodeLock.Unlock()

	if _, ok := rfs.directories[out.NodeId]; ok {
		panic(fmt.Sprintf("Leaf %d has the same node ID as an existing directory", out.NodeId))
	}

	// Increment lookup count of leaf.
	rfs.leaves[out.NodeId] = leafEntry{
		leaf:    i,
		nLookup: rfs.leaves[out.NodeId].nLookup + 1,
	}
}

func (rfs *simpleRawFileSystem) String() string {
	return "SimpleRawFileSystem"
}

func (rfs *simpleRawFileSystem) SetDebug(debug bool) {}

func (rfs *simpleRawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	directory, leaf, s := i.FUSELookup(path.MustNewComponent(name), &out.Attr)
	if s != fuse.OK {
		// TODO: Should we add support for generating negative
		// cache entries? Preliminary testing shows that this
		// doesn't seem effective, probably because build
		// actions are short lived.
		return s
	}
	if directory != nil {
		rfs.addDirectory(directory, out)
	} else {
		rfs.addLeaf(leaf, out)
	}
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Forget(nodeID, nLookup uint64) {
	rfs.nodeLock.Lock()
	defer rfs.nodeLock.Unlock()

	// Decrement lookup count of the directory or leaf node. We can
	// remove the entry from our bookkeeping if the lookup count
	// reaches zero.
	if entry, ok := rfs.directories[nodeID]; ok {
		if entry.nLookup < nLookup {
			panic(fmt.Sprintf("Attempted to forget directory %d %d times, while it was only looked up %d times", nodeID, nLookup, entry.nLookup))
		}
		entry.nLookup -= nLookup
		if entry.nLookup == 0 {
			delete(rfs.directories, nodeID)
		} else {
			rfs.directories[nodeID] = entry
		}
	} else if entry, ok := rfs.leaves[nodeID]; ok {
		if entry.nLookup < nLookup {
			panic(fmt.Sprintf("Attempted to forget leaf %d %d times, while it was only looked up %d times", nodeID, nLookup, entry.nLookup))
		}
		entry.nLookup -= nLookup
		if entry.nLookup == 0 {
			delete(rfs.leaves, nodeID)
		} else {
			rfs.leaves[nodeID] = entry
		}
	} else {
		panic(fmt.Sprintf("Attempted to forget node %d %d times, even though no directory or leaf under that ID exists", nodeID, nLookup))
	}
}

func (rfs *simpleRawFileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	i.FUSEGetAttr(&out.Attr)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	if s := i.FUSESetAttr(input, &out.Attr); s != fuse.OK {
		return s
	}
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	child, s := i.FUSEMknod(path.MustNewComponent(name), input.Mode, input.Rdev, &out.Attr)
	if s != fuse.OK {
		return s
	}
	rfs.addLeaf(child, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	child, s := i.FUSEMkdir(path.MustNewComponent(name), input.Mode, &out.Attr)
	if s != fuse.OK {
		return s
	}
	rfs.addDirectory(child, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEUnlink(path.MustNewComponent(name))
}

func (rfs *simpleRawFileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSERmdir(path.MustNewComponent(name))
}

func (rfs *simpleRawFileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
	rfs.nodeLock.RLock()
	iOld := rfs.getDirectoryLocked(input.NodeId)
	iNew := rfs.getDirectoryLocked(input.Newdir)
	rfs.nodeLock.RUnlock()

	return iOld.FUSERename(path.MustNewComponent(oldName), iNew, path.MustNewComponent(newName))
}

func (rfs *simpleRawFileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	iParent := rfs.getDirectoryLocked(input.NodeId)
	iChild := rfs.getLeafLocked(input.Oldnodeid)
	rfs.nodeLock.RUnlock()

	if s := iParent.FUSELink(path.MustNewComponent(filename), iChild, &out.Attr); s != fuse.OK {
		return s
	}
	rfs.addLeaf(iChild, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	child, s := i.FUSESymlink(pointedTo, path.MustNewComponent(linkName), &out.Attr)
	if s != fuse.OK {
		return s
	}
	rfs.addLeaf(child, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEReadlink()
}

func (rfs *simpleRawFileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEAccess(input.Mask)
}

func (rfs *simpleRawFileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	return 0, fuse.ENOATTR
}

func (rfs *simpleRawFileSystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	child, s := i.FUSECreate(path.MustNewComponent(name), input.Flags, input.Mode, &out.Attr)
	if s != fuse.OK {
		return s
	}
	rfs.addLeaf(child, &out.EntryOut)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEOpen(input.Flags)
}

func (rfs *simpleRawFileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSERead(buf, input.Offset)
}

func (rfs *simpleRawFileSystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	// TODO: Provide support for sparse files.
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

func (rfs *simpleRawFileSystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	i.FUSERelease()
}

func (rfs *simpleRawFileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEWrite(data, input.Offset)
}

func (rfs *simpleRawFileSystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return 0, fuse.ENOTSUP
}

func (rfs *simpleRawFileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	return i.FUSEFallocate(input.Offset, input.Length)
}

func (rfs *simpleRawFileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	dh := &directoryHandle{directory: i}
	rfs.directoryHandlesLock.Lock()
	out.Fh = rfs.nextDirectoryHandle
	rfs.nextDirectoryHandle++
	rfs.directoryHandles[out.Fh] = dh
	rfs.directoryHandlesLock.Unlock()
	return fuse.OK
}

// Directory entries that needed to be prepended to the results of all
// ReadDir() and ReadDirPlus() operations. The inode number is not
// filled in for these entries, which is permitted.
var dotDotEntries = []fuse.DirEntry{
	{Mode: fuse.S_IFDIR, Name: "."},
	{Mode: fuse.S_IFDIR, Name: ".."},
}

func (rfs *simpleRawFileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirEntryList) fuse.Status {
	rfs.directoryHandlesLock.RLock()
	dh := rfs.directoryHandles[input.Fh]
	rfs.directoryHandlesLock.RUnlock()

	dh.readDirLock.Lock()
	defer dh.readDirLock.Unlock()

	// Initial call. Load directory entries.
	if !dh.readDirInitialized || input.Offset == 0 {
		entries, s := dh.directory.FUSEReadDir()
		if s != fuse.OK {
			return s
		}
		dh.readDirInitialized = true
		dh.readDirEntries = entries
		rfs.directorySorter(dh.readDirEntries)
	}

	// Return "." and ".." entries, followed by entries for
	// directories and leaves. It's sufficient to just return the
	// metadata. There is no need to look up the nodes immediately.
	offset := input.Offset
	if l := uint64(len(dotDotEntries)); offset < l {
		for _, v := range dotDotEntries[offset:] {
			if !out.AddDirEntry(v) {
				return fuse.OK
			}
		}
		offset = 0
	} else {
		offset -= l
	}
	if offset < uint64(len(dh.readDirEntries)) {
		for _, v := range dh.readDirEntries[offset:] {
			if !out.AddDirEntry(v) {
				return fuse.OK
			}
		}
	}
	return fuse.OK
}

func (rfs *simpleRawFileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirPlusEntryList) fuse.Status {
	rfs.directoryHandlesLock.RLock()
	dh := rfs.directoryHandles[input.Fh]
	rfs.directoryHandlesLock.RUnlock()

	dh.readDirPlusLock.Lock()
	defer dh.readDirPlusLock.Unlock()

	// Initial call. Load directory entries.
	if !dh.readDirPlusInitialized || input.Offset == 0 {
		directories, leaves, s := dh.directory.FUSEReadDirPlus()
		if s != fuse.OK {
			return s
		}
		dh.readDirPlusInitialized = true
		dh.readDirPlusDirectories = directories
		rfs.directorySorter(dh.readDirPlusDirectories)
		dh.readDirPlusLeaves = leaves
		rfs.directorySorter(dh.readDirPlusLeaves)
	}

	// Return "." and ".." entries. These don't need to be looked
	// up, as the kernel tracks these for us automatically.
	offset := input.Offset
	if l := uint64(len(dotDotEntries)); offset < l {
		for _, v := range dotDotEntries[offset:] {
			if out.AddDirLookupEntry(v) == nil {
				return fuse.OK
			}
		}
		offset = 0
	} else {
		offset -= l
	}

	// Return entries for directories and leaves. These need to be
	// looked up immediately, so that the kernel has direct access.
	//
	// TODO: Would it make sense to batch all calls to
	// addDirectory() and addLeaf(), so that we only need to acquire
	// nodeLock once?
	if l := uint64(len(dh.readDirPlusDirectories)); offset < l {
		for _, v := range dh.readDirPlusDirectories[offset:] {
			e := out.AddDirLookupEntry(v.DirEntry)
			if e == nil {
				return fuse.OK
			}
			v.Child.FUSEGetAttr(&e.Attr)
			rfs.addDirectory(v.Child, e)
		}
		offset = 0
	} else {
		offset -= l
	}
	if offset < uint64(len(dh.readDirPlusLeaves)) {
		for _, v := range dh.readDirPlusLeaves[offset:] {
			e := out.AddDirLookupEntry(v.DirEntry)
			if e == nil {
				return fuse.OK
			}
			v.Child.FUSEGetAttr(&e.Attr)
			rfs.addLeaf(v.Child, e)
		}
	}
	return fuse.OK
}

func (rfs *simpleRawFileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	rfs.directoryHandlesLock.Lock()
	defer rfs.directoryHandlesLock.Unlock()

	if _, ok := rfs.directoryHandles[input.Fh]; !ok {
		panic("Attempted to release an unknown directory handle")
	}
	delete(rfs.directoryHandles, input.Fh)
}

func (rfs *simpleRawFileSystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (rfs *simpleRawFileSystem) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	// Announce support for filenames up to 255 bytes in size. This
	// seems to be the common limit for UNIX file systems. Setting
	// this value is necessary to make pathconf(path, _PC_NAME_MAX)
	// work.
	out.NameLen = 255
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Init(server fuse.ServerCallbacks) {
	sc := rfs.serverCallbacks
	sc.lock.Lock()
	sc.initializedServers = append(sc.initializedServers, initializedServer{
		rawFileSystem: rfs,
		server:        server,
	})
	sc.lock.Unlock()
}

// dirEntryList is a list of DirEntry objects returned by FUSEReadDir().
// This type may be used to sort elements in the list by name.
type dirEntryList []fuse.DirEntry

func (l dirEntryList) Len() int {
	return len(l)
}

func (l dirEntryList) Less(i, j int) bool {
	return l[i].Name < l[j].Name
}

func (l dirEntryList) Swap(i, j int) {
	t := l[i]
	l[i] = l[j]
	l[j] = t
}

// directoryDirEntryList is a list of DirectoryDirEntry objects returned
// by FUSEReadDirPlus(). This type may be used to sort elements in the
// list by name.
type directoryDirEntryList []DirectoryDirEntry

func (l directoryDirEntryList) Len() int {
	return len(l)
}

func (l directoryDirEntryList) Less(i, j int) bool {
	return l[i].DirEntry.Name < l[j].DirEntry.Name
}

func (l directoryDirEntryList) Swap(i, j int) {
	t := l[i]
	l[i] = l[j]
	l[j] = t
}

// leafDirEntryList is a list of LeafDirEntry objects returned by
// FUSEReadDirPlus(). This type may be used to sort elements in the list
// by name.
type leafDirEntryList []LeafDirEntry

func (l leafDirEntryList) Len() int {
	return len(l)
}

func (l leafDirEntryList) Less(i, j int) bool {
	return l[i].DirEntry.Name < l[j].DirEntry.Name
}

func (l leafDirEntryList) Swap(i, j int) {
	t := l[i]
	l[i] = l[j]
	l[j] = t
}
