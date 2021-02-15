// +build darwin linux

package fuse

import (
	"fmt"
	"sort"
	"sync"
	"syscall"

	re_sync "github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/atomic"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// inMemoryFilesystem contains state that is shared across all
// inMemoryPrepopulatedDirectory objects that form a single hierarchy.
type inMemoryFilesystem struct {
	entryNotifier        EntryNotifier
	inodeNumberGenerator random.ThreadSafeGenerator
	lastInodeSeed        atomic.Uint64
}

// inMemorySubtree contains state that is shared across all
// inMemoryPrepopulatedDirectory objects in a subtree of an
// inMemoryFilesystem.
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

// inMemoryDirectoryContents contains the listing of all children stored
// in an inMemoryPrepopulatedDirectory. The forbidNewChildren flag may
// be set when empty and no new children may be added.
type inMemoryDirectoryContents struct {
	directories       map[path.Component]*inMemoryPrepopulatedDirectory
	leaves            map[path.Component]NativeLeaf
	forbidNewChildren bool
}

// attachDirectory adds an existing directory to the directory contents.
func (c *inMemoryDirectoryContents) attachDirectory(name path.Component, child *inMemoryPrepopulatedDirectory) {
	if err := c.mayAttach(name); err != 0 {
		panic(fmt.Sprintf("Directory %#v may not be attached: %s", name, err))
	}
	c.directories[name] = child
}

// attachDirectory adds a new directory to the directory contents. The
// initial contents of this new directory may be specified in the form
// of an InitialContentsFetcher, which gets evaluated lazily.
func (c *inMemoryDirectoryContents) attachNewDirectory(subtree *inMemorySubtree, name path.Component, initialContentsFetcher InitialContentsFetcher) *inMemoryPrepopulatedDirectory {
	child := &inMemoryPrepopulatedDirectory{
		inodeNumber:            subtree.filesystem.inodeNumberGenerator.Uint64(),
		subtree:                subtree,
		initialContentsFetcher: initialContentsFetcher,
	}
	c.attachDirectory(name, child)
	return child
}

// attachLeaf adds a new leaf node to the directory contents.
func (c *inMemoryDirectoryContents) attachLeaf(name path.Component, child NativeLeaf) {
	if err := c.mayAttach(name); err != 0 {
		panic(fmt.Sprintf("Leaf %#v may not be attached: %s", name, err))
	}
	c.leaves[name] = child
}

func (c *inMemoryDirectoryContents) detachDirectory(name path.Component) {
	if _, ok := c.directories[name]; !ok {
		panic(fmt.Sprintf("Node %#v does not exist", name))
	}
	delete(c.directories, name)
}

func (c *inMemoryDirectoryContents) detachLeaf(name path.Component) {
	if _, ok := c.leaves[name]; !ok {
		panic(fmt.Sprintf("Node %#v does not exist", name))
	}
	delete(c.leaves, name)
}

// exists returns whether a directory already contains an entry with a
// given name.
func (c *inMemoryDirectoryContents) exists(name path.Component) bool {
	if _, ok := c.directories[name]; ok {
		return true
	}
	if _, ok := c.leaves[name]; ok {
		return true
	}
	return false
}

func (c *inMemoryDirectoryContents) mayAttach(name path.Component) syscall.Errno {
	if c.forbidNewChildren {
		return syscall.ENOENT
	}
	if c.exists(name) {
		return syscall.EEXIST
	}
	return 0
}

func (c *inMemoryDirectoryContents) isEmpty() bool {
	return len(c.directories) == 0 && len(c.leaves) == 0
}

func (c *inMemoryDirectoryContents) createChildren(subtree *inMemorySubtree, children map[path.Component]InitialNode) {
	for name, child := range children {
		if child.Directory != nil {
			c.attachNewDirectory(subtree, name, child.Directory)
		} else {
			c.attachLeaf(name, child.Leaf)
		}
	}
}

// getAndLockDirectory obtains a child directory from the current
// directory and immediately locks it. To prevent possible deadlocks, we
// must respect the lock order. This may require this function to drop
// the lock on current directories prior to picking up the lock of the
// child directory.
func (c *inMemoryDirectoryContents) getAndLockDirectory(name path.Component, lockPile *re_sync.LockPile) (*inMemoryPrepopulatedDirectory, bool) {
	for {
		child, ok := c.directories[name]
		if !ok {
			// No child node present.
			return nil, false
		}
		if lockPile.Lock(&child.lock) {
			// Lock acquisition of child succeeded without
			// dropping any of the existing locks.
			return child, true
		}
		if c.directories[name] == child {
			// Even though we dropped locks, no race occurred.
			return child, true
		}
		lockPile.Unlock(&child.lock)
	}
}

// inMemoryPrepopulatedDirectory is an implementation of PrepopulatedDirectory that
// keeps all directory metadata stored in memory. Actual file data and
// metadata is not managed by this implementation. Files are allocated
// by calling into a provided FileAllocator.
//
// inMemoryPrepopulatedDirectory uses fine-grained locking. Every directory has
// its own mutex that protects its maps of child directories and leaf
// nodes.  As various operations require the acquisition of multiple
// locks (e.g., Rename() locking up to three directories), util.LockPile
// is used for deadlock avoidance. To ensure consistency, locks on one
// or more directories may be held when calling into the FileAllocator
// or NativeLeaf nodes.
type inMemoryPrepopulatedDirectory struct {
	subtree     *inMemorySubtree
	inodeNumber uint64

	lock                   sync.Mutex
	initialContentsFetcher InitialContentsFetcher
	contents               inMemoryDirectoryContents
}

// NewInMemoryPrepopulatedDirectory creates a new PrepopulatedDirectory
// that keeps all directory metadata stored in memory. As the filesystem
// API does not allow traversing the hierarchy upwards, this directory
// can be considered the root directory of the hierarchy.
func NewInMemoryPrepopulatedDirectory(fileAllocator FileAllocator, errorLogger util.ErrorLogger, rootInodeNumber uint64, inodeNumberGenerator random.ThreadSafeGenerator, entryNotifier EntryNotifier) PrepopulatedDirectory {
	return &inMemoryPrepopulatedDirectory{
		subtree: &inMemorySubtree{
			filesystem: &inMemoryFilesystem{
				entryNotifier:        entryNotifier,
				inodeNumberGenerator: inodeNumberGenerator,
			},
			fileAllocator: fileAllocator,
			errorLogger:   errorLogger,
		},
		inodeNumber:            rootInodeNumber,
		initialContentsFetcher: EmptyInitialContentsFetcher,
	}
}

// Initialize the directory with the intended contents if not done so
// already. This function is used by inMemoryPrepopulatedDirectory's operations
// to gain access to the directory's contents.
func (i *inMemoryPrepopulatedDirectory) getContents() (*inMemoryDirectoryContents, error) {
	if i.initialContentsFetcher != nil {
		children, err := i.initialContentsFetcher.FetchContents()
		if err != nil {
			return nil, err
		}
		i.initialContentsFetcher = nil
		i.contents.directories = map[path.Component]*inMemoryPrepopulatedDirectory{}
		i.contents.leaves = map[path.Component]NativeLeaf{}
		i.contents.createChildren(i.subtree, children)
	}
	return &i.contents, nil
}

// Invalidate a directory entry exposed through the FUSE file system.
// This function needs to be called when removing entries through the
// PrepopulatedDirectory interface, to ensure that files become absent
// through the mount point as well.
//
// This function must not be called while holding any
// inMemoryPrepopulatedDirectory or FUSE locks.
func (i *inMemoryPrepopulatedDirectory) invalidate(name path.Component) {
	i.subtree.filesystem.entryNotifier(i.inodeNumber, name)
}

func (i *inMemoryPrepopulatedDirectory) LookupChild(name path.Component) (PrepopulatedDirectory, NativeLeaf, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, err := i.getContents()
	if err != nil {
		return nil, nil, err
	}

	if child, ok := contents.directories[name]; ok {
		return child, nil, nil
	}
	if child, ok := contents.leaves[name]; ok {
		return nil, child, nil
	}
	return nil, nil, syscall.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, err := i.getContents()
	if err != nil {
		return nil, err
	}

	var entries filesystem.FileInfoList
	for name := range contents.directories {
		entries = append(entries,
			filesystem.NewFileInfo(name, filesystem.FileTypeDirectory))
	}
	for name, leaf := range contents.leaves {
		entries = append(entries,
			filesystem.NewFileInfo(name, leaf.GetFileType()))
	}
	sort.Sort(entries)
	return entries, nil
}

func (i *inMemoryPrepopulatedDirectory) Remove(name path.Component) error {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, err := i.getContents()
	if err != nil {
		return err
	}

	if child, ok := contents.getAndLockDirectory(name, &lockPile); ok {
		// The directory has a child directory under that name.
		// Perform an rmdir().
		childContents, err := child.getContents()
		if err != nil {
			return err
		}
		if !childContents.isEmpty() {
			return syscall.ENOTEMPTY
		}
		contents.detachDirectory(name)
		childContents.forbidNewChildren = true
		lockPile.UnlockAll()
		i.invalidate(name)
		return nil
	}

	if child, ok := contents.leaves[name]; ok {
		// The directory has a child file/symlink under that
		// name. Perform an unlink().
		contents.detachLeaf(name)
		child.Unlink()
		lockPile.UnlockAll()
		i.invalidate(name)
		return nil
	}

	return syscall.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) RemoveAll(name path.Component) error {
	i.lock.Lock()

	contents, err := i.getContents()
	if err != nil {
		i.lock.Unlock()
		return err
	}

	if child, ok := contents.directories[name]; ok {
		// The directory has a child directory under that name.
		// Perform a recursive removal.
		contents.detachDirectory(name)
		i.lock.Unlock()
		i.invalidate(name)
		child.removeAllChildren(true)
		return nil
	}

	if child, ok := contents.leaves[name]; ok {
		// The directory has a child file/symlink under that
		// name. Perform an unlink().
		contents.detachLeaf(name)
		i.lock.Unlock()
		child.Unlink()
		i.invalidate(name)
		return nil
	}

	i.lock.Unlock()
	return syscall.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) RemoveAllChildren(forbidNewChildren bool) error {
	i.removeAllChildren(forbidNewChildren)
	return nil
}

func (i *inMemoryPrepopulatedDirectory) removeAllChildren(forbidNewChildren bool) {
	i.lock.Lock()
	if i.initialContentsFetcher != nil {
		// The directory has not been initialized. Instead of
		// initializing it as intended and removing all
		// contents, forcefully initialize it as an empty
		// directory.
		i.initialContentsFetcher = nil
		i.contents = inMemoryDirectoryContents{
			directories:       map[path.Component]*inMemoryPrepopulatedDirectory{},
			leaves:            map[path.Component]NativeLeaf{},
			forbidNewChildren: forbidNewChildren,
		}
		i.lock.Unlock()
	} else {
		contents := &i.contents
		if forbidNewChildren {
			contents.forbidNewChildren = true
		}

		// Detach all contents from the directory.
		names := make([]path.Component, 0, len(contents.directories)+len(contents.leaves))
		directories := make([]*inMemoryPrepopulatedDirectory, 0, len(contents.directories))
		for name, child := range contents.directories {
			contents.detachDirectory(name)
			names = append(names, name)
			directories = append(directories, child)
		}
		leaves := make([]NativeLeaf, 0, len(contents.leaves))
		for name, child := range contents.leaves {
			contents.detachLeaf(name)
			names = append(names, name)
			leaves = append(leaves, child)
		}
		i.lock.Unlock()

		i.postRemoveChildren(names, directories, leaves)
	}
}

// postRemoveChildren is called after bulk unlinking files and
// directories and dropping the parent directory lock. It invalidates
// all entries in the FUSE directory entry cache and recursively removes
// all files.
func (i *inMemoryPrepopulatedDirectory) postRemoveChildren(names []path.Component, directories []*inMemoryPrepopulatedDirectory, leaves []NativeLeaf) {
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

func (i *inMemoryPrepopulatedDirectory) InstallHooks(fileAllocator FileAllocator, errorLogger util.ErrorLogger) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.subtree = &inMemorySubtree{
		filesystem:    i.subtree.filesystem,
		fileAllocator: fileAllocator,
		errorLogger:   errorLogger,
	}
}

func (i *inMemoryPrepopulatedDirectory) CreateChildren(children map[path.Component]InitialNode, overwrite bool) error {
	i.lock.Lock()
	contents, err := i.getContents()
	if err != nil {
		i.lock.Unlock()
		return err
	}

	if contents.forbidNewChildren {
		i.lock.Unlock()
		return syscall.ENOENT
	}

	// Remove entries that are about to be overwritten.
	var names []path.Component
	var directories []*inMemoryPrepopulatedDirectory
	var leaves []NativeLeaf
	if overwrite {
		for name := range children {
			if child, ok := contents.directories[name]; ok {
				contents.detachDirectory(name)
				names = append(names, name)
				directories = append(directories, child)
			} else if child, ok := contents.leaves[name]; ok {
				contents.detachLeaf(name)
				names = append(names, name)
				leaves = append(leaves, child)
			}
		}
	} else {
		for name := range children {
			if contents.exists(name) {
				i.lock.Unlock()
				return syscall.EEXIST
			}
		}
	}

	contents.createChildren(i.subtree, children)
	i.lock.Unlock()

	i.postRemoveChildren(names, directories, leaves)
	return nil
}

func (i *inMemoryPrepopulatedDirectory) CreateAndEnterPrepopulatedDirectory(name path.Component) (PrepopulatedDirectory, error) {
	i.lock.Lock()

	contents, err := i.getContents()
	if err != nil {
		i.lock.Unlock()
		return nil, err
	}

	if child, ok := contents.directories[name]; ok {
		i.lock.Unlock()
		return child, nil
	}

	if oldChild, ok := contents.leaves[name]; ok {
		contents.detachLeaf(name)
		oldChild.Unlink()
		newChild := contents.attachNewDirectory(i.subtree, name, EmptyInitialContentsFetcher)
		i.lock.Unlock()
		i.invalidate(name)
		return newChild, nil
	}

	if contents.forbidNewChildren {
		return nil, syscall.ENOENT
	}
	child := contents.attachNewDirectory(i.subtree, name, EmptyInitialContentsFetcher)
	i.lock.Unlock()
	return child, nil
}

func (i *inMemoryPrepopulatedDirectory) filterChildrenRecursive(childFilter ChildFilter) bool {
	i.lock.Lock()
	if initialContentsFetcher := i.initialContentsFetcher; initialContentsFetcher != nil {
		// Directory is not initialized. There is no need to
		// instantiate it. Simply provide the
		// InitialContentsFetcher to the callback.
		i.lock.Unlock()
		return childFilter(InitialNode{Directory: initialContentsFetcher}, func() error {
			return i.RemoveAllChildren(false)
		})
	}

	// Directory is already initialized. Gather the contents.
	type leafInfo struct {
		name path.Component
		leaf NativeLeaf
	}
	leaves := make([]leafInfo, 0, len(i.contents.leaves))
	for name, child := range i.contents.leaves {
		leaves = append(leaves, leafInfo{
			name: name,
			leaf: child,
		})
	}

	directories := make([]*inMemoryPrepopulatedDirectory, 0, len(i.contents.directories))
	for _, child := range i.contents.directories {
		directories = append(directories, child)
	}
	i.lock.Unlock()

	// Invoke the callback for all children.
	for _, child := range leaves {
		name := child.name
		if !childFilter(InitialNode{Leaf: child.leaf}, func() error {
			return i.Remove(name)
		}) {
			return false
		}
	}
	for _, child := range directories {
		if !child.filterChildrenRecursive(childFilter) {
			return false
		}
	}
	return true
}

func (i *inMemoryPrepopulatedDirectory) FilterChildren(childFilter ChildFilter) error {
	i.filterChildrenRecursive(childFilter)
	return nil
}

func (i *inMemoryPrepopulatedDirectory) fuseGetContents() (*inMemoryDirectoryContents, fuse.Status) {
	contents, err := i.getContents()
	if err != nil {
		i.subtree.errorLogger.Log(util.StatusWrap(err, "Failed to initialize directory"))
		return nil, fuse.EIO
	}
	return contents, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEAccess(mask uint32) fuse.Status {
	return fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSECreate(name path.Component, flags, mode uint32, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, s
	}

	if err := contents.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}

	child, s := i.subtree.fileAllocator.NewFile(flags, mode)
	if s != fuse.OK {
		return nil, s
	}

	contents.attachLeaf(name, child)
	child.FUSEGetAttr(out)
	return child, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = i.inodeNumber
	out.Mode = fuse.S_IFDIR | 0o777
	// To be consistent with traditional UNIX file systems, this
	// would need to be 2 + len(i.directories), but that would
	// require us to initialize the directory, which is undesirable.
	out.Nlink = ImplicitDirectoryLinkCount
}

func (i *inMemoryPrepopulatedDirectory) FUSELink(name path.Component, leaf Leaf, out *fuse.Attr) fuse.Status {
	child, ok := leaf.(NativeLeaf)
	if !ok {
		// The file is not the kind that can be embedded into
		// inMemoryPrepopulatedDirectory.
		return fuse.EXDEV
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return s
	}

	if err := contents.mayAttach(name); err != 0 {
		return fuse.Status(err)
	}
	child.Link()
	contents.attachLeaf(name, child)

	child.FUSEGetAttr(out)
	return fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSELookup(name path.Component, out *fuse.Attr) (Directory, Leaf, fuse.Status) {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, nil, s
	}

	if child, ok := contents.directories[name]; ok {
		child.FUSEGetAttr(out)
		return child, nil, fuse.OK
	}
	if child, ok := contents.leaves[name]; ok {
		child.FUSEGetAttr(out)
		return nil, child, fuse.OK
	}
	return nil, nil, fuse.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) FUSEMkdir(name path.Component, mode uint32, out *fuse.Attr) (Directory, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, s
	}

	if err := contents.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}
	child := contents.attachNewDirectory(i.subtree, name, EmptyInitialContentsFetcher)

	child.FUSEGetAttr(out)
	return child, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEMknod(name path.Component, mode, dev uint32, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, s
	}

	if err := contents.mayAttach(name); err != 0 {
		return nil, fuse.Status(err)
	}
	if fileType := mode & syscall.S_IFMT; fileType != fuse.S_IFIFO && fileType != syscall.S_IFSOCK {
		return nil, fuse.EPERM
	}
	child := NewDevice(i.subtree.filesystem.inodeNumberGenerator.Uint64(), mode, dev)
	contents.attachLeaf(name, child)

	child.FUSEGetAttr(out)
	return child, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEReadDir() ([]fuse.DirEntry, fuse.Status) {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, s
	}

	entries := make([]fuse.DirEntry, 0, len(contents.directories)+len(contents.leaves))
	for name, child := range contents.directories {
		entries = append(entries, fuse.DirEntry{
			Mode: fuse.S_IFDIR,
			Ino:  child.inodeNumber,
			Name: name.String(),
		})
	}
	for name, child := range contents.leaves {
		directoryEntry := child.FUSEGetDirEntry()
		directoryEntry.Name = name.String()
		entries = append(entries, directoryEntry)
	}

	return entries, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEReadDirPlus() ([]DirectoryDirEntry, []LeafDirEntry, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, nil, s
	}

	directories := make([]DirectoryDirEntry, 0, len(contents.directories))
	for name, child := range contents.directories {
		directories = append(directories, DirectoryDirEntry{
			Child: child,
			DirEntry: fuse.DirEntry{
				Mode: fuse.S_IFDIR,
				Ino:  child.inodeNumber,
				Name: name.String(),
			},
		})
	}

	leaves := make([]LeafDirEntry, 0, len(contents.leaves))
	for name, child := range contents.leaves {
		directoryEntry := child.FUSEGetDirEntry()
		directoryEntry.Name = name.String()
		leaves = append(leaves, LeafDirEntry{
			Child:    child,
			DirEntry: directoryEntry,
		})
	}
	return directories, leaves, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSERename(oldName path.Component, newDirectory Directory, newName path.Component) fuse.Status {
	iOld := i
	iNew, ok := newDirectory.(*inMemoryPrepopulatedDirectory)
	if !ok {
		return fuse.EXDEV
	}

	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&iOld.lock, &iNew.lock)

	oldContents, s := iOld.fuseGetContents()
	if s != fuse.OK {
		return s
	}
	newContents, s := iNew.fuseGetContents()
	if s != fuse.OK {
		return s
	}

	// Renaming to a location at which a directory already exists.
	if newChild, ok := newContents.getAndLockDirectory(newName, &lockPile); ok {
		if _, ok := oldContents.leaves[oldName]; ok {
			return fuse.EISDIR
		}
		if oldChild, ok := oldContents.directories[oldName]; ok {
			// Renaming a directory to itself is always
			// permitted, even when not empty.
			if newChild == oldChild {
				return fuse.OK
			}
			if iOld.subtree.filesystem != iNew.subtree.filesystem {
				return fuse.EXDEV
			}
			newChildContents, s := newChild.fuseGetContents()
			if s != fuse.OK {
				return s
			}
			if !newChildContents.isEmpty() {
				return fuse.Status(syscall.ENOTEMPTY)
			}
			oldContents.detachDirectory(oldName)
			// TODO: Pick up an interlock and check for
			// potential creation of cyclic directory
			// structures.
			newContents.detachDirectory(newName)
			newChildContents.forbidNewChildren = true
			newContents.attachDirectory(newName, oldChild)
			return fuse.OK
		}
		return fuse.ENOENT
	}

	// Renaming to a location at which a leaf already exists.
	if newChild, ok := newContents.leaves[newName]; ok {
		if oldChild, ok := oldContents.leaves[oldName]; ok {
			// POSIX requires that renaming a file to itself
			// has no effect. After running the following
			// sequence of commands, both "a" and "b" should
			// still exist: "touch a; ln a b; mv a b".
			if newChild == oldChild {
				return fuse.OK
			}
			oldContents.detachLeaf(oldName)
			newContents.detachLeaf(newName)
			newChild.Unlink()
			newContents.attachLeaf(newName, oldChild)
			return fuse.OK
		}
		if _, ok := oldContents.directories[oldName]; ok {
			return fuse.ENOTDIR
		}
		return fuse.ENOENT
	}

	// Renaming to a location where no file exists.
	if newContents.forbidNewChildren {
		return fuse.ENOENT
	}
	if oldChild, ok := oldContents.leaves[oldName]; ok {
		oldContents.detachLeaf(oldName)
		newContents.attachLeaf(newName, oldChild)
		return fuse.OK
	}
	if oldChild, ok := oldContents.directories[oldName]; ok {
		if iOld.subtree.filesystem != iNew.subtree.filesystem {
			return fuse.EXDEV
		}
		oldContents.detachDirectory(oldName)
		newContents.attachDirectory(newName, oldChild)
		return fuse.OK
	}
	return fuse.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) FUSERmdir(name path.Component) fuse.Status {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return s
	}

	if child, ok := contents.getAndLockDirectory(name, &lockPile); ok {
		childContents, s := child.fuseGetContents()
		if s != fuse.OK {
			return s
		}
		if !childContents.isEmpty() {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		contents.detachDirectory(name)
		childContents.forbidNewChildren = true
		return fuse.OK
	}

	if _, ok := contents.leaves[name]; ok {
		return fuse.ENOTDIR
	}

	return fuse.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EINVAL
	}
	i.FUSEGetAttr(out)
	return fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSESymlink(pointedTo string, linkName path.Component, out *fuse.Attr) (Leaf, fuse.Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return nil, s
	}

	if err := contents.mayAttach(linkName); err != 0 {
		return nil, fuse.Status(err)
	}
	child := NewSymlink(pointedTo)
	contents.attachLeaf(linkName, child)

	child.FUSEGetAttr(out)
	return child, fuse.OK
}

func (i *inMemoryPrepopulatedDirectory) FUSEUnlink(name path.Component) fuse.Status {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.fuseGetContents()
	if s != fuse.OK {
		return s
	}

	if child, ok := contents.leaves[name]; ok {
		contents.detachLeaf(name)
		child.Unlink()
		return fuse.OK
	}
	if _, ok := contents.directories[name]; ok {
		return fuse.EPERM
	}
	return fuse.ENOENT
}
