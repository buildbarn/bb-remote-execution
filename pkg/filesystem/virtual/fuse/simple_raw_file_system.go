//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"
	"fmt"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"

	"golang.org/x/sys/unix"
)

const (
	// AttributesMaskForFUSEAttr is the attributes mask to use for
	// VirtualGetAttributes() to populate all relevant fields of
	// fuse.Attr.
	AttributesMaskForFUSEAttr = virtual.AttributesMaskDeviceNumber |
		virtual.AttributesMaskFileType |
		virtual.AttributesMaskInodeNumber |
		virtual.AttributesMaskLastDataModificationTime |
		virtual.AttributesMaskLinkCount |
		virtual.AttributesMaskPermissions |
		virtual.AttributesMaskSizeBytes
	// AttributesMaskForFUSEDirEntry is the attributes mask to use
	// for VirtualReadDir() to populate all relevant fields of
	// fuse.DirEntry.
	AttributesMaskForFUSEDirEntry = virtual.AttributesMaskFileType |
		virtual.AttributesMaskInodeNumber
)

func toFUSEStatus(s virtual.Status) fuse.Status {
	switch s {
	case virtual.StatusOK:
		return fuse.OK
	case virtual.StatusErrAccess:
		return fuse.EACCES
	case virtual.StatusErrExist:
		return fuse.Status(syscall.EEXIST)
	case virtual.StatusErrInval:
		return fuse.EINVAL
	case virtual.StatusErrIO:
		return fuse.EIO
	case virtual.StatusErrIsDir:
		return fuse.EISDIR
	case virtual.StatusErrNoEnt:
		return fuse.ENOENT
	case virtual.StatusErrNotDir:
		return fuse.ENOTDIR
	case virtual.StatusErrNotEmpty:
		return fuse.Status(syscall.ENOTEMPTY)
	case virtual.StatusErrNXIO:
		return fuse.Status(syscall.ENXIO)
	case virtual.StatusErrPerm:
		return fuse.EPERM
	case virtual.StatusErrROFS:
		return fuse.EROFS
	case virtual.StatusErrStale:
		return fuse.Status(syscall.ESTALE)
	case virtual.StatusErrSymlink:
		return fuse.Status(syscall.EOPNOTSUPP)
	case virtual.StatusErrWrongType:
		return fuse.EBADF
	case virtual.StatusErrXDev:
		return fuse.EXDEV
	default:
		panic("Unknown status")
	}
}

type directoryEntry struct {
	directory virtual.Directory
	nLookup   uint64
}

type leafEntry struct {
	leaf    virtual.Leaf
	nLookup uint64
}

type simpleRawFileSystem struct {
	removalNotifierRegistrar virtual.FUSERemovalNotifierRegistrar
	authenticator            Authenticator

	// Maps to resolve node IDs to directories and leaves.
	nodeLock    sync.RWMutex
	directories map[uint64]directoryEntry
	leaves      map[uint64]leafEntry
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
func NewSimpleRawFileSystem(rootDirectory virtual.Directory, removalNotifierRegistrar virtual.FUSERemovalNotifierRegistrar, authenticator Authenticator) fuse.RawFileSystem {
	return &simpleRawFileSystem{
		removalNotifierRegistrar: removalNotifierRegistrar,
		authenticator:            authenticator,

		directories: map[uint64]directoryEntry{
			fuse.FUSE_ROOT_ID: {
				directory: rootDirectory,
				nLookup:   1,
			},
		},
		leaves: map[uint64]leafEntry{},
	}
}

func toFUSEFileType(fileType filesystem.FileType) uint32 {
	switch fileType {
	case filesystem.FileTypeBlockDevice:
		return syscall.S_IFBLK
	case filesystem.FileTypeCharacterDevice:
		return syscall.S_IFCHR
	case filesystem.FileTypeDirectory:
		return syscall.S_IFDIR
	case filesystem.FileTypeFIFO:
		return syscall.S_IFIFO
	case filesystem.FileTypeRegularFile:
		return syscall.S_IFREG
	case filesystem.FileTypeSocket:
		return syscall.S_IFSOCK
	case filesystem.FileTypeSymlink:
		return syscall.S_IFLNK
	default:
		panic("Unknown file type")
	}
}

func populateAttr(attributes *virtual.Attributes, out *fuse.Attr) {
	if deviceNumber, ok := attributes.GetDeviceNumber(); ok {
		out.Rdev = uint32(deviceNumber.ToRaw())
	}

	out.Ino = attributes.GetInodeNumber()
	out.Nlink = attributes.GetLinkCount()
	out.Mode = toFUSEFileType(attributes.GetFileType())

	if lastDataModificationTime, ok := attributes.GetLastDataModificationTime(); ok {
		nanos := lastDataModificationTime.UnixNano()
		out.Mtime = uint64(nanos / 1e9)
		out.Mtimensec = uint32(nanos % 1e9)
	}

	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Attributes do not contain mandatory permissions attribute")
	}
	out.Mode |= uint32(permissions.ToMode())

	sizeBytes, ok := attributes.GetSizeBytes()
	if !ok {
		panic("Attributes do not contain mandatory size attribute")
	}
	out.Size = sizeBytes
}

func populateEntryOut(attributes *virtual.Attributes, out *fuse.EntryOut) {
	populateAttr(attributes, &out.Attr)
	out.NodeId = out.Ino
}

func (rfs *simpleRawFileSystem) getDirectoryLocked(nodeID uint64) virtual.Directory {
	if entry, ok := rfs.directories[nodeID]; ok {
		return entry.directory
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known directory", nodeID))
}

func (rfs *simpleRawFileSystem) getLeafLocked(nodeID uint64) virtual.Leaf {
	if entry, ok := rfs.leaves[nodeID]; ok {
		return entry.leaf
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known leaf", nodeID))
}

func (rfs *simpleRawFileSystem) getNodeLocked(nodeID uint64) virtual.Node {
	if entry, ok := rfs.directories[nodeID]; ok {
		return entry.directory
	}
	if entry, ok := rfs.leaves[nodeID]; ok {
		return entry.leaf
	}
	panic(fmt.Sprintf("Node ID %d does not correspond to a known directory or leaf", nodeID))
}

func (rfs *simpleRawFileSystem) addDirectory(i virtual.Directory, attributes *virtual.Attributes, out *fuse.EntryOut) {
	populateEntryOut(attributes, out)

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

func (rfs *simpleRawFileSystem) addLeaf(i virtual.Leaf, attributes *virtual.Attributes, out *fuse.EntryOut) {
	populateEntryOut(attributes, out)

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

// channelBackedContext is an implementation of context.Context around
// the cancellation channel that go-fuse provides. It does not have any
// values or deadline associated with it.
type channelBackedContext struct {
	cancel <-chan struct{}
}

var _ context.Context = channelBackedContext{}

func (ctx channelBackedContext) Deadline() (time.Time, bool) {
	var t time.Time
	return t, false
}

func (ctx channelBackedContext) Done() <-chan struct{} {
	return ctx.cancel
}

func (ctx channelBackedContext) Err() error {
	select {
	case <-ctx.cancel:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx channelBackedContext) Value(key any) any {
	return nil
}

func (rfs *simpleRawFileSystem) createContext(cancel <-chan struct{}, caller *fuse.Caller) (context.Context, fuse.Status) {
	return rfs.authenticator.Authenticate(channelBackedContext{cancel: cancel}, caller)
}

func (rfs *simpleRawFileSystem) String() string {
	return "SimpleRawFileSystem"
}

func (rfs *simpleRawFileSystem) SetDebug(debug bool) {}

func (rfs *simpleRawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &header.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	child, vs := i.VirtualLookup(ctx, path.MustNewComponent(name), AttributesMaskForFUSEAttr, &attributes)
	if vs != virtual.StatusOK {
		// TODO: Should we add support for generating negative
		// cache entries? Preliminary testing shows that this
		// doesn't seem effective, probably because build
		// actions are short lived.
		return toFUSEStatus(vs)
	}
	if directory, leaf := child.GetPair(); directory != nil {
		rfs.addDirectory(directory, &attributes, out)
	} else {
		rfs.addLeaf(leaf, &attributes, out)
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
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	i.VirtualGetAttributes(ctx, AttributesMaskForFUSEAttr, &attributes)
	populateAttr(&attributes, &out.Attr)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var attributesIn virtual.Attributes
	if input.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if input.Valid&fuse.FATTR_MODE != 0 {
		attributesIn.SetPermissions(virtual.NewPermissionsFromMode(input.Mode))
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		attributesIn.SetSizeBytes(input.Size)
	}

	var attributesOut virtual.Attributes
	if s := i.VirtualSetAttributes(ctx, &attributesIn, AttributesMaskForFUSEAttr, &attributesOut); s != virtual.StatusOK {
		return toFUSEStatus(s)
	}
	populateAttr(&attributesOut, &out.Attr)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var fileType filesystem.FileType
	switch input.Mode & syscall.S_IFMT {
	case syscall.S_IFIFO:
		fileType = filesystem.FileTypeFIFO
	case syscall.S_IFSOCK:
		fileType = filesystem.FileTypeSocket
	default:
		return fuse.EPERM
	}

	var attributes virtual.Attributes
	child, _, vs := i.VirtualMknod(ctx, path.MustNewComponent(name), fileType, AttributesMaskForFUSEAttr, &attributes)
	if vs != virtual.StatusOK {
		return toFUSEStatus(vs)
	}
	rfs.addLeaf(child, &attributes, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	child, _, s := i.VirtualMkdir(path.MustNewComponent(name), AttributesMaskForFUSEAttr, &attributes)
	if s != virtual.StatusOK {
		return toFUSEStatus(s)
	}
	rfs.addDirectory(child, &attributes, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	_, s := i.VirtualRemove(path.MustNewComponent(name), false, true)
	return toFUSEStatus(s)
}

func (rfs *simpleRawFileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	_, s := i.VirtualRemove(path.MustNewComponent(name), true, false)
	return toFUSEStatus(s)
}

func (rfs *simpleRawFileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
	rfs.nodeLock.RLock()
	iOld := rfs.getDirectoryLocked(input.NodeId)
	iNew := rfs.getDirectoryLocked(input.Newdir)
	rfs.nodeLock.RUnlock()

	_, _, s := iOld.VirtualRename(path.MustNewComponent(oldName), iNew, path.MustNewComponent(newName))
	return toFUSEStatus(s)
}

func (rfs *simpleRawFileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	iParent := rfs.getDirectoryLocked(input.NodeId)
	iChild := rfs.getLeafLocked(input.Oldnodeid)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	if _, s := iParent.VirtualLink(ctx, path.MustNewComponent(filename), iChild, AttributesMaskForFUSEAttr, &attributes); s != virtual.StatusOK {
		return toFUSEStatus(s)
	}
	rfs.addLeaf(iChild, &attributes, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &header.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	child, _, vs := i.VirtualSymlink(ctx, []byte(pointedTo), path.MustNewComponent(linkName), AttributesMaskForFUSEAttr, &attributes)
	if vs != virtual.StatusOK {
		return toFUSEStatus(vs)
	}
	rfs.addLeaf(child, &attributes, out)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	ctx, s := rfs.createContext(cancel, &header.Caller)
	if s != fuse.OK {
		return nil, s
	}

	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(header.NodeId)
	rfs.nodeLock.RUnlock()

	target, vs := i.VirtualReadlink(ctx)
	return target, toFUSEStatus(vs)
}

func (rfs *simpleRawFileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getNodeLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var requiredPermissions virtual.Permissions
	if input.Mask&fuse.R_OK != 0 {
		requiredPermissions |= virtual.PermissionsRead
	}
	if input.Mask&fuse.W_OK != 0 {
		requiredPermissions |= virtual.PermissionsWrite
	}
	if input.Mask&fuse.X_OK != 0 {
		requiredPermissions |= virtual.PermissionsExecute
	}

	var attributes virtual.Attributes
	i.VirtualGetAttributes(ctx, virtual.AttributesMaskPermissions, &attributes)
	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Node did not return permissions attribute, even though it was requested")
	}
	if requiredPermissions&^permissions != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (rfs *simpleRawFileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	// By returning ENOSYS here, the Linux FUSE driver will set
	// fuse_conn::no_getxattr. This will completely eliminate
	// getxattr() calls going forward. More details:
	//
	// https://github.com/torvalds/linux/blob/371e8fd02969383204b1f6023451125dbc20dfbd/fs/fuse/xattr.c#L60-L61
	// https://github.com/torvalds/linux/blob/371e8fd02969383204b1f6023451125dbc20dfbd/fs/fuse/xattr.c#L85-L88
	//
	// Similar logic is used for some of the other operations.
	return 0, fuse.ENOSYS
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

// oflagsToShareMask converts access modes stored in open() flags to a
// ShareMask, indicating which operations are expected to be called
// against the file descriptor.
func oflagsToShareMask(oflags uint32) (virtual.ShareMask, fuse.Status) {
	switch oflags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		return virtual.ShareMaskRead, fuse.OK
	case syscall.O_WRONLY:
		return virtual.ShareMaskWrite, fuse.OK
	case syscall.O_RDWR:
		return virtual.ShareMaskRead | virtual.ShareMaskWrite, fuse.OK
	default:
		return 0, fuse.EINVAL
	}
}

// oflagsToOpenExistingOptions converts options stored in open() flags
// that pertain to handling of existing files to an OpenExistingOptions
// struct, which may be provided to VirtualOpen*().
func oflagsToOpenExistingOptions(oflags uint32, options *virtual.OpenExistingOptions) {
	options.Truncate = oflags&syscall.O_TRUNC != 0
}

func (rfs *simpleRawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	// Translate access mode.
	shareAccess, s := oflagsToShareMask(input.Flags)
	if s != fuse.OK {
		return s
	}

	// Take O_EXCL and O_TRUNC flags into consideration.
	var existingOptions *virtual.OpenExistingOptions
	if input.Flags&syscall.O_EXCL == 0 {
		existingOptions = &virtual.OpenExistingOptions{}
		oflagsToOpenExistingOptions(input.Flags, existingOptions)
	}

	var openedFileAttributes virtual.Attributes
	child, _, _, vs := i.VirtualOpenChild(
		ctx,
		path.MustNewComponent(name),
		shareAccess,
		(&virtual.Attributes{}).SetPermissions(virtual.NewPermissionsFromMode(input.Mode)),
		existingOptions,
		AttributesMaskForFUSEAttr,
		&openedFileAttributes)
	if vs != virtual.StatusOK {
		return toFUSEStatus(vs)
	}
	rfs.addLeaf(child, &openedFileAttributes, &out.EntryOut)
	return fuse.OK
}

func (rfs *simpleRawFileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	shareAccess, s := oflagsToShareMask(input.Flags)
	if s != fuse.OK {
		return s
	}
	var options virtual.OpenExistingOptions
	oflagsToOpenExistingOptions(input.Flags, &options)

	return toFUSEStatus(i.VirtualOpenSelf(ctx, shareAccess, &options, 0, &virtual.Attributes{}))
}

func (rfs *simpleRawFileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	nRead, _, s := i.VirtualRead(buf, input.Offset)
	if s != virtual.StatusOK {
		return nil, toFUSEStatus(s)
	}
	return fuse.ReadResultData(buf[:nRead]), fuse.OK
}

func (rfs *simpleRawFileSystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(in.NodeId)
	rfs.nodeLock.RUnlock()

	var regionType filesystem.RegionType
	switch in.Whence {
	case unix.SEEK_DATA:
		regionType = filesystem.Data
	case unix.SEEK_HOLE:
		regionType = filesystem.Hole
	default:
		panic("Requests for other seek modes should have been intercepted")
	}

	offset, s := i.VirtualSeek(in.Offset, regionType)
	if s != virtual.StatusOK {
		return toFUSEStatus(s)
	}
	if offset == nil {
		return fuse.Status(syscall.ENXIO)
	}
	out.Offset = *offset
	return fuse.OK
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

	i.VirtualClose(1)
}

func (rfs *simpleRawFileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	rfs.nodeLock.RLock()
	i := rfs.getLeafLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	n, s := i.VirtualWrite(data, input.Offset)
	return uint32(n), toFUSEStatus(s)
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

	return toFUSEStatus(i.VirtualAllocate(input.Offset, input.Length))
}

func (rfs *simpleRawFileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()

	var attributes virtual.Attributes
	i.VirtualGetAttributes(ctx, virtual.AttributesMaskPermissions, &attributes)
	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Node did not return permissions attribute, even though it was requested")
	}
	if permissions&virtual.PermissionsRead == 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

// Directory entries that needed to be prepended to the results of all
// ReadDir() and ReadDirPlus() operations. The inode number is not
// filled in for these entries, which is permitted.
var dotDotEntries = []fuse.DirEntry{
	{Mode: fuse.S_IFDIR, Name: "."},
	{Mode: fuse.S_IFDIR, Name: ".."},
}

const dotDotEntriesCount uint64 = 2

func toFUSEDirEntry(name path.Component, attributes *virtual.Attributes) fuse.DirEntry {
	return fuse.DirEntry{
		Mode: toFUSEFileType(attributes.GetFileType()),
		Name: name.String(),
		Ino:  attributes.GetInodeNumber(),
	}
}

type readDirReporter struct {
	out fuse.ReadDirEntryList
}

func (r *readDirReporter) ReportEntry(nextCookie uint64, name path.Component, child virtual.DirectoryChild, attributes *virtual.Attributes) bool {
	return r.out.AddDirEntry(toFUSEDirEntry(name, attributes), dotDotEntriesCount+nextCookie)
}

func (rfs *simpleRawFileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirEntryList) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	// Inject "." and ".." entries at the start of the results.
	offset := input.Offset
	for ; offset < dotDotEntriesCount; offset++ {
		if !out.AddDirEntry(dotDotEntries[offset], offset+1) {
			return fuse.OK
		}
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()
	return toFUSEStatus(
		i.VirtualReadDir(
			ctx,
			offset-dotDotEntriesCount,
			AttributesMaskForFUSEDirEntry,
			&readDirReporter{out: out}))
}

type readDirPlusReporter struct {
	rfs *simpleRawFileSystem
	out fuse.ReadDirPlusEntryList
}

func (r *readDirPlusReporter) ReportEntry(nextCookie uint64, name path.Component, child virtual.DirectoryChild, attributes *virtual.Attributes) bool {
	if e := r.out.AddDirLookupEntry(toFUSEDirEntry(name, attributes), dotDotEntriesCount+nextCookie); e != nil {
		if directory, leaf := child.GetPair(); directory != nil {
			r.rfs.addDirectory(directory, attributes, e)
		} else {
			r.rfs.addLeaf(leaf, attributes, e)
		}
		return true
	}
	return false
}

func (rfs *simpleRawFileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirPlusEntryList) fuse.Status {
	ctx, s := rfs.createContext(cancel, &input.Caller)
	if s != fuse.OK {
		return s
	}

	// Return "." and ".." entries at the start of the results.
	// These don't need to be looked up, as the kernel tracks these
	// for us automatically.
	offset := input.Offset
	for ; offset < dotDotEntriesCount; offset++ {
		if out.AddDirLookupEntry(dotDotEntries[offset], offset+1) == nil {
			return fuse.OK
		}
	}

	rfs.nodeLock.RLock()
	i := rfs.getDirectoryLocked(input.NodeId)
	rfs.nodeLock.RUnlock()
	return toFUSEStatus(
		i.VirtualReadDir(
			ctx,
			offset-dotDotEntriesCount,
			AttributesMaskForFUSEAttr,
			&readDirPlusReporter{rfs: rfs, out: out}))
}

func (rfs *simpleRawFileSystem) ReleaseDir(input *fuse.ReleaseIn) {}

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
	// Obtain the inode number of the root directory.
	rfs.nodeLock.RLock()
	rootDirectory := rfs.directories[fuse.FUSE_ROOT_ID].directory
	rfs.nodeLock.RUnlock()
	var attributes virtual.Attributes
	rootDirectory.VirtualGetAttributes(context.Background(), virtual.AttributesMaskInodeNumber, &attributes)
	rootDirectoryInodeNumber := attributes.GetInodeNumber()

	rfs.removalNotifierRegistrar(func(parent uint64, name path.Component) {
		// EntryNotify can be called to report that directory entries
		// have been removed. This causes them to be removed from the
		// directory entry cache used by FUSE as well.
		if parent == rootDirectoryInodeNumber {
			// Even though we permit the root directory to
			// have an arbitrary inode number, FUSE requires
			// that the root directory uses node ID 1.
			if s := server.EntryNotify(fuse.FUSE_ROOT_ID, name.String()); s != fuse.OK && s != fuse.ENOENT {
				log.Printf("Failed to invalidate %#v in root directory: %s", name.String(), s)
			}
		} else {
			// Discard invalidations for directory entries
			// if the containing directory isn't known to
			// the kernel. These requests would fail anyway.
			rfs.nodeLock.RLock()
			_, ok := rfs.directories[parent]
			rfs.nodeLock.RUnlock()
			if ok {
				if s := server.EntryNotify(parent, name.String()); s != fuse.OK && s != fuse.ENOENT {
					log.Printf("Failed to invalidate %#v in directory %d: %s", name.String(), parent, s)
				}
			}
		}
	})
}
