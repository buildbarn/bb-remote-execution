// +build darwin linux

package fuse

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"

	re_sync "github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/atomic"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// inMemoryFilesystem contains state that is shared across all
// InMemoryDirectory objects that form a single hierarchy.
type inMemoryFilesystem struct {
	entryNotifier   EntryNotifier
	inodeNumberTree InodeNumberTree
	lastInodeSeed   atomic.Uint64
}

func (imf *inMemoryFilesystem) newInodeNumber() uint64 {
	return imf.inodeNumberTree.AddUint64(imf.lastInodeSeed.Add(1)).Get()
}

// inMemorySubtree contains state that is shared across all
// InMemoryDirectory objects in a subtree of an inMemoryFilesystem.
//
// Every subtree in the filesystem may have its own file allocator. This
// permits us to apply per-action disk quotas. It may also have its own
// error logger, which allows us to notify LocalBuildExecutor of disk
// I/O errors.
type inMemorySubtree struct {
	filesystem    *inMemoryFilesystem
	fileAllocator FileAllocator
	errorLogger   util.ErrorLogger
}

// InMemoryDirectory is an implementation of filesystem.Directory that
// keeps all directory metadata stored in memory. Actual file data and
// metadata is not managed by this implementation. Files are allocated
// by calling into a provided FileAllocator.
//
// There are two features that are specific to this directory
// implementation. First of all, it is possible to export directories
// through FUSE. The GetFUSENode() function can be used to obtain a
// handle that may be passed to the go-fuse library.
//
// Second, it supports the creation of directories that are initialized
// lazily. This could, for example, be used to obtain build directories
// that are directly backed by the Content Addressable Storage. Instead
// of loading all of the contents up front, directories are only loaded
// and expanded when accessed. This is useful for running build actions
// that have large input roots, of which only a fraction is actually
// accessed.
//
// InMemoryDirectory uses fine-grained locking. Every directory has its
// own mutex that protects its maps of child directories and leaf nodes.
// As various operations require the acquisition of multiple locks
// (e.g., Rename() locking up to three directories), util.LockPile is
// used for deadlock avoidance. To ensure consistency, locks on one or
// more directories may be held when calling into the FileAllocator or
// NativeLeaf nodes.
type InMemoryDirectory struct {
	subtree     *inMemorySubtree
	inodeNumber uint64

	lock                   sync.Mutex
	initialContentsFetcher InitialContentsFetcher
	directories            map[path.Component]*InMemoryDirectory
	leaves                 map[path.Component]NativeLeaf
	isDeleted              bool
}

var _ Directory = (*InMemoryDirectory)(nil)

// NewInMemoryDirectory creates a new directory. As the filesystem API
// does not allow traversing the hierarchy upwards, this directory can
// be considered the root directory of the hierarchy.
func NewInMemoryDirectory(fileAllocator FileAllocator, errorLogger util.ErrorLogger, inodeNumberTree InodeNumberTree, entryNotifier EntryNotifier) *InMemoryDirectory {
	return &InMemoryDirectory{
		subtree: &inMemorySubtree{
			filesystem: &inMemoryFilesystem{
				entryNotifier:   entryNotifier,
				inodeNumberTree: inodeNumberTree,
			},
			fileAllocator: fileAllocator,
			errorLogger:   errorLogger,
		},
		inodeNumber: inodeNumberTree.Get(),
		directories: map[path.Component]*InMemoryDirectory{},
		leaves:      map[path.Component]NativeLeaf{},
	}
}

// Initialize the directory with the intended contents if not done so
// already. This function is used by InMemoryDirectory's operations to
// gain access to the directory's contents.
//
// For directories created through NewInMemoryDirectory() and Mkdir(),
// this function has no effect. It is only necessary to be called on
// directories that are defined implicitly (i.e., created through
// MergeDirectoryContents).
func (i *InMemoryDirectory) initialize() error {
	if i.initialContentsFetcher != nil {
		directories, leaves, err := i.initialContentsFetcher.FetchContents()
		if err != nil {
			return err
		}
		i.directories = map[path.Component]*InMemoryDirectory{}
		i.leaves = map[path.Component]NativeLeaf{}
		if err := i.mergeDirectoryContents(directories, leaves); err != nil {
			i.directories = nil
			i.leaves = nil
			return err
		}
		i.initialContentsFetcher = nil
	}
	return nil
}

func (i *InMemoryDirectory) attachDirectory(name path.Component, child *InMemoryDirectory) {
	if err := i.mayAttach(name); err != 0 {
		panic(fmt.Sprintf("Directory %#v may not be attached: %s", name, err))
	}
	if child.isDeleted {
		panic(fmt.Sprintf("Attempted to add deleted directory %#v to directory", name))
	}
	i.directories[name] = child
}

func (i *InMemoryDirectory) attachEmptyDirectory(name path.Component) *InMemoryDirectory {
	child := &InMemoryDirectory{
		inodeNumber: i.subtree.filesystem.newInodeNumber(),
		subtree:     i.subtree,
		directories: map[path.Component]*InMemoryDirectory{},
		leaves:      map[path.Component]NativeLeaf{},
	}
	i.attachDirectory(name, child)
	return child
}

func (i *InMemoryDirectory) attachLeaf(name path.Component, child NativeLeaf) {
	if err := i.mayAttach(name); err != 0 {
		panic(fmt.Sprintf("Leaf %#v may not be attached: %s", name, err))
	}
	i.leaves[name] = child
}

func (i *InMemoryDirectory) detachDirectory(name path.Component) {
	if _, ok := i.directories[name]; !ok {
		panic(fmt.Sprintf("Node %#v does not exist", name))
	}
	delete(i.directories, name)
}

func (i *InMemoryDirectory) detachLeaf(name path.Component) {
	if _, ok := i.leaves[name]; !ok {
		panic(fmt.Sprintf("Node %#v does not exist", name))
	}
	delete(i.leaves, name)
}

func (i *InMemoryDirectory) exists(name path.Component) bool {
	if _, ok := i.directories[name]; ok {
		return true
	}
	if _, ok := i.leaves[name]; ok {
		return true
	}
	return false
}

func (i *InMemoryDirectory) mayAttach(name path.Component) syscall.Errno {
	if i.isDeleted {
		return syscall.ENOENT
	}
	if i.exists(name) {
		return syscall.EEXIST
	}
	return 0
}

func (i *InMemoryDirectory) isEmpty() bool {
	return len(i.directories) == 0 && len(i.leaves) == 0
}

// getAndLockDirectory obtains a child directory from the current
// directory and immediately locks it. To prevent possible deadlocks, we
// must respect the lock order. This may require this function to drop
// the lock on current directories prior to picking up the lock of the
// child directory.
func (i *InMemoryDirectory) getAndLockDirectory(name path.Component, lockPile *re_sync.LockPile) (*InMemoryDirectory, bool) {
	for {
		child, ok := i.directories[name]
		if !ok {
			// No child node present.
			return nil, false
		}
		if lockPile.Lock(&child.lock) {
			// Lock acquisition of child succeeded without
			// dropping any of the existing locks.
			return child, true
		}
		if i.directories[name] == child {
			// Even though we dropped locks, no race occurred.
			return child, true
		}
		lockPile.Unlock(&child.lock)
	}
}

func (i *InMemoryDirectory) mergeDirectoryContents(directories map[path.Component]InitialContentsFetcher, leaves map[path.Component]NativeLeaf) error {
	for name, initialContentFetcher := range directories {
		if i.exists(name) {
			return status.Errorf(codes.AlreadyExists, "Directory already contains a node with name %#v", name)
		}
		i.attachDirectory(
			name,
			&InMemoryDirectory{
				inodeNumber:            i.subtree.filesystem.newInodeNumber(),
				subtree:                i.subtree,
				initialContentsFetcher: initialContentFetcher,
			})
	}
	for name, leaf := range leaves {
		if i.exists(name) {
			return status.Errorf(codes.AlreadyExists, "Directory already contains a node with name %#v", name)
		}
		i.attachLeaf(name, leaf)
	}
	return nil
}

// Invalidate a directory entry exposed through the FUSE file system.
// This function needs to be called when removing entries through the
// filesystem.Directory interface, to ensure that files become absent
// through the mount point as well.
//
// This function must not be called while holding any InMemoryDirectory
// or FUSE locks.
func (i *InMemoryDirectory) invalidate(name path.Component) {
	if s := i.subtree.filesystem.entryNotifier(i.inodeNumber, name); s != fuse.OK && s != fuse.ENOENT {
		log.Printf("Failed to invalidate %#v in directory %d: %s", name, i.inodeNumber, s)
	}
}

// EnterInMemoryDirectory enters an existing directory contained within
// the current directory, returning a handle to it.
func (i *InMemoryDirectory) EnterInMemoryDirectory(name path.Component) (*InMemoryDirectory, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return nil, err
	}
	if child, ok := i.directories[name]; ok {
		return child, nil
	}
	if _, ok := i.leaves[name]; ok {
		return nil, syscall.ENOTDIR
	}
	return nil, syscall.ENOENT
}

// Lstat obtains the attributes of a file stored within the directory.
func (i *InMemoryDirectory) Lstat(name path.Component) (filesystem.FileInfo, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return filesystem.FileInfo{}, err
	}
	if _, ok := i.directories[name]; ok {
		return filesystem.NewFileInfo(name, filesystem.FileTypeDirectory), nil
	}
	if child, ok := i.leaves[name]; ok {
		return filesystem.NewFileInfo(name, child.GetFileType()), nil
	}
	return filesystem.FileInfo{}, syscall.ENOENT
}

// Mkdir creates a directory with a given name within the current
// directory.
func (i *InMemoryDirectory) Mkdir(name path.Component, perm os.FileMode) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return err
	}
	if err := i.mayAttach(name); err != 0 {
		return err
	}
	i.attachEmptyDirectory(name)
	return nil
}

// Mknod creates a device node with a given name within the current
// directory. For our use case, it only needs to support creation of
// character devices.
func (i *InMemoryDirectory) Mknod(name path.Component, perm os.FileMode, dev int) error {
	if perm&os.ModeType != os.ModeDevice|os.ModeCharDevice {
		return status.Error(codes.InvalidArgument, "The provided file mode is not for a character device")
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return err
	}
	if err := i.mayAttach(name); err != 0 {
		return err
	}
	child := NewDevice(i.subtree.filesystem.newInodeNumber(), syscall.S_IFCHR, uint32(dev))
	i.attachLeaf(name, child)
	return nil
}

// ReadDir returns a list of all directories and leaves contained within
// the directory.
func (i *InMemoryDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return nil, err
	}

	var entries filesystem.FileInfoList
	for name := range i.directories {
		entries = append(entries,
			filesystem.NewFileInfo(name, filesystem.FileTypeDirectory))
	}
	for name, leaf := range i.leaves {
		entries = append(entries,
			filesystem.NewFileInfo(name, leaf.GetFileType()))
	}
	sort.Sort(entries)
	return entries, nil
}

// Readlink reads the symlink target of a leaf node contained within the
// directory.
func (i *InMemoryDirectory) Readlink(name path.Component) (string, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return "", err
	}

	if child, ok := i.leaves[name]; ok {
		return child.Readlink()
	}
	if _, ok := i.directories[name]; ok {
		return "", syscall.EINVAL
	}
	return "", syscall.ENOENT
}

// Remove a single leaf or empty directory from the current directory.
func (i *InMemoryDirectory) Remove(name path.Component) error {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	if err := i.initialize(); err != nil {
		return err
	}

	if child, ok := i.getAndLockDirectory(name, &lockPile); ok {
		// The directory has a child directory under that name.
		// Perform an rmdir().
		if err := child.initialize(); err != nil {
			return err
		}
		if !child.isEmpty() {
			return syscall.ENOTEMPTY
		}
		i.detachDirectory(name)
		child.isDeleted = true
		lockPile.UnlockAll()
		i.invalidate(name)
		return nil
	}

	if child, ok := i.leaves[name]; ok {
		// The directory has a child file/symlink under that
		// name. Perform an unlink().
		i.detachLeaf(name)
		child.Unlink()
		lockPile.UnlockAll()
		i.invalidate(name)
		return nil
	}

	return syscall.ENOENT
}

// RemoveAll removes a single leaf or directory from the current
// directory. When removing a directory, it is deleted recursively.
func (i *InMemoryDirectory) RemoveAll(name path.Component) error {
	i.lock.Lock()
	if err := i.initialize(); err != nil {
		i.lock.Unlock()
		return err
	}

	if child, ok := i.directories[name]; ok {
		// The directory has a child directory under that name.
		// Perform a recursive removal.
		i.detachDirectory(name)
		i.lock.Unlock()
		child.removeAllChildren(true)
		i.invalidate(name)
		return nil
	}

	if child, ok := i.leaves[name]; ok {
		// The directory has a child file/symlink under that
		// name. Perform an unlink().
		i.detachLeaf(name)
		i.lock.Unlock()
		child.Unlink()
		i.invalidate(name)
		return nil
	}

	i.lock.Unlock()
	return syscall.ENOENT
}

// RemoveAllChildren empties out a directory.
func (i *InMemoryDirectory) RemoveAllChildren() error {
	i.removeAllChildren(false)
	return nil
}

func (i *InMemoryDirectory) removeAllChildren(deleteParent bool) {
	i.lock.Lock()
	if deleteParent {
		i.isDeleted = true
	}
	if i.initialContentsFetcher != nil {
		// The directory has not been initialized. Instead of
		// initializing it as intended and removing all
		// children, forcefully initialize it as an empty
		// directory.
		i.initialContentsFetcher = nil
		i.directories = map[path.Component]*InMemoryDirectory{}
		i.leaves = map[path.Component]NativeLeaf{}
		i.lock.Unlock()
	} else {
		// Detach all children from the directory.
		names := make([]path.Component, 0, len(i.directories)+len(i.leaves))
		directories := make([]*InMemoryDirectory, 0, len(i.directories))
		for name, child := range i.directories {
			i.detachDirectory(name)
			names = append(names, name)
			directories = append(directories, child)
		}
		leaves := make([]NativeLeaf, 0, len(i.leaves))
		for name, child := range i.leaves {
			i.detachLeaf(name)
			names = append(names, name)
			leaves = append(leaves, child)
		}
		i.lock.Unlock()

		// Recursively unlink all children.
		for _, name := range names {
			i.invalidate(name)
		}
		for _, directory := range directories {
			directory.removeAllChildren(true)
		}
		for _, leaf := range leaves {
			leaf.Unlink()
		}
	}
}

// InstallHooks replaces the FileAllocator and ErrorLogger used by this
// directory and any new files and directories created within.
func (i *InMemoryDirectory) InstallHooks(fileAllocator FileAllocator, errorLogger util.ErrorLogger) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.subtree = &inMemorySubtree{
		filesystem:    i.subtree.filesystem,
		fileAllocator: fileAllocator,
		errorLogger:   errorLogger,
	}
}

// MergeDirectoryContents merges an existing set of lazily instantiated
// directories and leaves into the current directory. In the case of
// Buildbarn's worker, this is used to project the input root on top of
// an empty build directory.
func (i *InMemoryDirectory) MergeDirectoryContents(directories map[path.Component]InitialContentsFetcher, leaves map[path.Component]NativeLeaf) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.initialize(); err != nil {
		return err
	}
	if i.isDeleted {
		return status.Error(codes.InvalidArgument, "Cannot merge contents into a directory that has already been deleted")
	}
	return i.mergeDirectoryContents(directories, leaves)
}

// UploadFile uploads a file contained in the current directory into the
// Content Addressable Storage. File types are permitted to implement
// this in optimized ways. For example, a file that is already backed by
// the Content Addressable Storage may implement this as a no-op.
func (i *InMemoryDirectory) UploadFile(ctx context.Context, name path.Component, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	i.lock.Lock()
	if child, ok := i.leaves[name]; ok {
		// Temporary increase the link count of the file, so
		// that child.UploadFile() can be called without holding
		// the directory lock.
		child.Link()
		i.lock.Unlock()
		childDigest, err := child.UploadFile(ctx, contentAddressableStorage, digestFunction)
		child.Unlink()
		return childDigest, err
	}
	if _, ok := i.directories[name]; ok {
		i.lock.Unlock()
		return digest.BadDigest, syscall.EISDIR
	}
	i.lock.Unlock()
	return digest.BadDigest, syscall.ENOENT
}

// FUSEAccess checks whether certain operations are permitted on this
// directory. Because this implementation does not support any
// permissions, this call always succeeds.
func (i *InMemoryDirectory) FUSEAccess(mask uint32) fuse.Status {
	return fuse.OK
}

// FUSECreate creates a file within the directory.
func (i *InMemoryDirectory) FUSECreate(name path.Component, flags, mode uint32, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}

	child, err := i.subtree.fileAllocator.NewFile(
		i.subtree.filesystem.newInodeNumber(),
		os.FileMode(mode))
	if err != nil {
		i.subtree.errorLogger.Log(util.StatusWrapf(err, "Failed to create new file with name %#v", name.String()))
		return nil, fuse.EIO
	}

	if s := child.FUSEOpen(flags); s != fuse.OK {
		// Undo the creation of the file.
		child.Unlink()
		return nil, s
	}

	i.attachLeaf(name, child)
	child.FUSEGetAttr(out)
	return child, fuse.OK
}

func (i *InMemoryDirectory) fuseGetAttrLocked(out *fuse.Attr) {
	out.Ino = i.inodeNumber
	out.Mode = fuse.S_IFDIR | 0777
	out.Nlink = uint32(len(i.directories) + 2)
}

// FUSEGetAttr obtains status attributes of the directory.
func (i *InMemoryDirectory) FUSEGetAttr(out *fuse.Attr) {
	i.lock.Lock()
	i.fuseGetAttrLocked(out)
	i.lock.Unlock()
}

// FUSEGetXAttr obtains extended attributes of the directory.
func (i *InMemoryDirectory) FUSEGetXAttr(attr string, dest []byte) (uint32, fuse.Status) {
	return 0, fuse.ENOATTR
}

// FUSELink links an existing file into the directory.
func (i *InMemoryDirectory) FUSELink(name path.Component, leaf Leaf, out *fuse.Attr) fuse.Status {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.mayAttach(name); err != 0 {
		return fuse.Status(err)
	}

	child := leaf.(NativeLeaf)
	child.Link()

	i.attachLeaf(name, child)
	child.FUSEGetAttr(out)
	return fuse.OK
}

// FUSELookup obtains the inode corresponding with a child stored within
// the directory.
func (i *InMemoryDirectory) FUSELookup(name path.Component, out *fuse.Attr) (Directory, Leaf, fuse.Status) {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	if child, ok := i.getAndLockDirectory(name, &lockPile); ok {
		// Ensure proper initialization of the directory for
		// fuseGetAttrLocked(). This also makes the
		// implementations of all FUSE operations simpler, as
		// they don't need to initialize themselves.
		if err := child.initialize(); err != nil {
			child.subtree.errorLogger.Log(util.StatusWrap(err, "Failed to initialize directory during lookup"))
			return nil, nil, fuse.EIO
		}
		child.fuseGetAttrLocked(out)
		return child, nil, fuse.OK
	}
	if child, ok := i.leaves[name]; ok {
		child.FUSEGetAttr(out)
		return nil, child, fuse.OK
	}
	return nil, nil, fuse.ENOENT
}

// FUSEMkdir creates a child directory within the current directory.
func (i *InMemoryDirectory) FUSEMkdir(name path.Component, mode uint32, out *fuse.Attr) (Directory, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}
	child := i.attachEmptyDirectory(name)

	child.fuseGetAttrLocked(out)
	return child, fuse.OK
}

// FUSEMknod creates a FIFO or UNIX domain socket within the current
// directory. It does not permit the creation of character and block
// devices.
func (i *InMemoryDirectory) FUSEMknod(name path.Component, mode, dev uint32, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}
	if fileType := mode & syscall.S_IFMT; fileType != fuse.S_IFIFO && fileType != syscall.S_IFSOCK {
		return nil, fuse.EPERM
	}
	child := NewDevice(i.subtree.filesystem.newInodeNumber(), mode, dev)
	i.attachLeaf(name, child)

	child.FUSEGetAttr(out)
	return child, fuse.OK
}

// FUSEReadDir returns a list of all files and directories stored within
// the directory.
func (i *InMemoryDirectory) FUSEReadDir() ([]fuse.DirEntry, fuse.Status) {
	lockPile := re_sync.LockPile{}
	lockPile.Lock(&i.lock)

	entries := make([]fuse.DirEntry, 0, len(i.directories)+len(i.leaves))
	for name, child := range i.directories {
		entries = append(entries, fuse.DirEntry{
			Mode: fuse.S_IFDIR,
			Ino:  child.inodeNumber,
			Name: name.String(),
		})
	}
	for name, child := range i.leaves {
		directoryEntry := child.FUSEGetDirEntry()
		directoryEntry.Name = name.String()
		entries = append(entries, directoryEntry)
	}

	lockPile.UnlockAll()
	return entries, fuse.OK
}

// FUSEReadDirPlus returns a list of all files and directories stored
// within the directory. In addition to that, it returns handles to each
// of the entries, so that there is no need to call FUSELookup on them
// separately.
func (i *InMemoryDirectory) FUSEReadDirPlus() ([]DirectoryDirEntry, []LeafDirEntry, fuse.Status) {
	lockPile := re_sync.LockPile{}
	lockPile.Lock(&i.lock)

	directories := make([]DirectoryDirEntry, 0, len(i.directories))
	directoriesToInitialize := make([]*InMemoryDirectory, 0, len(i.directories))
	for name, child := range i.directories {
		directories = append(directories, DirectoryDirEntry{
			Child: child,
			DirEntry: fuse.DirEntry{
				Mode: fuse.S_IFDIR,
				Ino:  child.inodeNumber,
				Name: name.String(),
			},
		})
		directoriesToInitialize = append(directoriesToInitialize, child)
	}

	leaves := make([]LeafDirEntry, 0, len(i.leaves))
	for name, child := range i.leaves {
		directoryEntry := child.FUSEGetDirEntry()
		directoryEntry.Name = name.String()
		leaves = append(leaves, LeafDirEntry{
			Child:    child,
			DirEntry: directoryEntry,
		})
	}

	lockPile.UnlockAll()

	// Similar to FUSELookup(), we must ensure that all directory
	// handles that are returned are properly initialized.
	for i, child := range directoriesToInitialize {
		child.lock.Lock()
		err := child.initialize()
		child.lock.Unlock()
		if err != nil {
			child.subtree.errorLogger.Log(
				util.StatusWrapf(err, "Failed to initialize directory %#v during readdir", directories[i].DirEntry.Name))
			return nil, nil, fuse.EIO
		}
	}
	return directories, leaves, fuse.OK
}

// FUSERename renames a file stored in the current directory,
// potentially moving it to another directory.
func (i *InMemoryDirectory) FUSERename(oldName path.Component, newDirectory Directory, newName path.Component) fuse.Status {
	iOld := i
	iNew, ok := newDirectory.(*InMemoryDirectory)
	if !ok {
		return fuse.EXDEV
	}

	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&iOld.lock, &iNew.lock)

	// Renaming to a location at which a directory already exists.
	if newChild, ok := iNew.getAndLockDirectory(newName, &lockPile); ok {
		if _, ok := iOld.leaves[oldName]; ok {
			return fuse.EISDIR
		}
		if oldChild, ok := iOld.directories[oldName]; ok {
			// Renaming a directory to itself is always
			// permitted, even when not empty.
			if newChild == oldChild {
				return fuse.OK
			}
			if iOld.subtree.filesystem != iNew.subtree.filesystem {
				return fuse.EXDEV
			}
			if err := newChild.initialize(); err != nil {
				newChild.subtree.errorLogger.Log(util.StatusWrap(err, "Failed to initialize directory during rename"))
				return fuse.EIO
			}
			if !newChild.isEmpty() {
				return fuse.Status(syscall.ENOTEMPTY)
			}
			iOld.detachDirectory(oldName)
			// TODO: Pick up an interlock and check for
			// potential creation of cyclic directory
			// structures.
			iNew.detachDirectory(newName)
			newChild.isDeleted = true
			iNew.attachDirectory(newName, oldChild)
			return fuse.OK
		}
		return fuse.ENOENT
	}

	// Renaming to a location at which a leaf already exists.
	if newChild, ok := iNew.leaves[newName]; ok {
		if oldChild, ok := iOld.leaves[oldName]; ok {
			// POSIX requires that renaming a file to itself
			// has no effect. After running the following
			// sequence of commands, both "a" and "b" should
			// still exist: "touch a; ln a b; mv a b".
			if newChild == oldChild {
				return fuse.OK
			}
			iOld.detachLeaf(oldName)
			iNew.detachLeaf(newName)
			newChild.Unlink()
			iNew.attachLeaf(newName, oldChild)
			return fuse.OK
		}
		if _, ok := iOld.directories[oldName]; ok {
			return fuse.ENOTDIR
		}
		return fuse.ENOENT
	}

	// Renaming to a location where no file exists.
	if iNew.isDeleted {
		return fuse.ENOENT
	}
	if oldChild, ok := iOld.leaves[oldName]; ok {
		iOld.detachLeaf(oldName)
		iNew.attachLeaf(newName, oldChild)
		return fuse.OK
	}
	if oldChild, ok := iOld.directories[oldName]; ok {
		if iOld.subtree.filesystem != iNew.subtree.filesystem {
			return fuse.EXDEV
		}
		iOld.detachDirectory(oldName)
		iNew.attachDirectory(newName, oldChild)
		return fuse.OK
	}
	return fuse.ENOENT
}

// FUSERmdir removes an empty directory stored within the current
// directory.
func (i *InMemoryDirectory) FUSERmdir(name path.Component) fuse.Status {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	if child, ok := i.getAndLockDirectory(name, &lockPile); ok {
		if err := child.initialize(); err != nil {
			child.subtree.errorLogger.Log(util.StatusWrap(err, "Failed to initialize directory during removal"))
			return fuse.EIO
		}
		if !child.isEmpty() {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		i.detachDirectory(name)
		child.isDeleted = true
		return fuse.OK
	}

	if _, ok := i.leaves[name]; ok {
		return fuse.ENOTDIR
	}

	return fuse.ENOENT
}

// FUSESetAttr modifies status attributes of the directory.
func (i *InMemoryDirectory) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EINVAL
	}
	i.FUSEGetAttr(out)
	return fuse.OK
}

// FUSESymlink creates a symbolic link within the current directory.
func (i *InMemoryDirectory) FUSESymlink(pointedTo string, linkName path.Component, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := i.mayAttach(linkName); err != 0 {
		return nil, fuse.Status(err)
	}
	child := NewSymlink(pointedTo)
	i.attachLeaf(linkName, child)

	child.FUSEGetAttr(out)
	return child, fuse.OK
}

// FUSEUnlink removes a leaf node from the current directory.
func (i *InMemoryDirectory) FUSEUnlink(name path.Component) fuse.Status {
	i.lock.Lock()
	defer i.lock.Unlock()

	if child, ok := i.leaves[name]; ok {
		i.detachLeaf(name)
		child.Unlink()
		return fuse.OK
	}
	if _, ok := i.directories[name]; ok {
		return fuse.EPERM
	}
	return fuse.ENOENT
}
