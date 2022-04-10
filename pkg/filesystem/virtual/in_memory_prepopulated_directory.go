package virtual

import (
	"fmt"
	"sort"
	"sync"
	"syscall"

	re_sync "github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// inMemoryFilesystem contains state that is shared across all
// inMemoryPrepopulatedDirectory objects that form a single hierarchy.
type inMemoryFilesystem struct {
	symlinkFactory          SymlinkFactory
	statefulHandleAllocator StatefulHandleAllocator
	initialContentsSorter   Sorter
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

func (s *inMemorySubtree) createNewDirectory(initialContentsFetcher InitialContentsFetcher) *inMemoryPrepopulatedDirectory {
	d := &inMemoryPrepopulatedDirectory{
		subtree:                s,
		initialContentsFetcher: initialContentsFetcher,
	}
	d.handle = s.filesystem.statefulHandleAllocator.New().AsStatefulDirectory(d)
	return d
}

// inMemoryDirectoryChild contains exactly one reference to an object
// that's embedded in a parent directory.
type inMemoryDirectoryChild struct {
	directory *inMemoryPrepopulatedDirectory
	leaf      NativeLeaf
}

func (c *inMemoryDirectoryChild) isDirectory() bool {
	return c.directory != nil
}

// inMemoryDirectoryEntry is a directory entry for an object stored in
// inMemoryDirectoryContents.
type inMemoryDirectoryEntry struct {
	child inMemoryDirectoryChild

	// For VirtualReadDir().
	cookie   uint64
	name     path.Component
	previous *inMemoryDirectoryEntry
	next     *inMemoryDirectoryEntry
}

// inMemoryDirectoryContents contains the listing of all children stored
// in an inMemoryPrepopulatedDirectory. Entries are stored both in a map
// and a list. The latter is needed for readdir() to behave
// deterministically. The isDeleted flag may be set when empty and no
// new children may be added.
type inMemoryDirectoryContents struct {
	entriesMap  map[path.Component]*inMemoryDirectoryEntry
	entriesList inMemoryDirectoryEntry
	isDeleted   bool
	changeID    uint64
}

// initialize a directory by making it empty.
func (c *inMemoryDirectoryContents) initialize() {
	c.entriesMap = map[path.Component]*inMemoryDirectoryEntry{}
	c.entriesList.previous = &c.entriesList
	c.entriesList.next = &c.entriesList
}

// attach an existing directory or leaf to the directory contents.
func (c *inMemoryDirectoryContents) attach(name path.Component, child inMemoryDirectoryChild) {
	if err := c.mayAttach(name); err != 0 {
		panic(fmt.Sprintf("Directory %#v may not be attached: %s", name, err))
	}
	entry := &inMemoryDirectoryEntry{
		child: child,

		name:     name,
		cookie:   c.changeID,
		previous: c.entriesList.previous,
		next:     &c.entriesList,
	}
	c.entriesMap[name] = entry
	entry.previous.next = entry
	entry.next.previous = entry
	c.changeID++
}

// attachDirectory adds a new directory to the directory contents. The
// initial contents of this new directory may be specified in the form
// of an InitialContentsFetcher, which gets evaluated lazily.
func (c *inMemoryDirectoryContents) attachNewDirectory(subtree *inMemorySubtree, name path.Component, initialContentsFetcher InitialContentsFetcher) *inMemoryPrepopulatedDirectory {
	newDirectory := subtree.createNewDirectory(initialContentsFetcher)
	c.attach(name, inMemoryDirectoryChild{directory: newDirectory})
	return newDirectory
}

// Detach the entry from the directory. Clear the entry to prevent
// foot-shooting. This allows VirtualReadDir() to detect that iteration
// was interrupted.
func (c *inMemoryDirectoryContents) detach(entry *inMemoryDirectoryEntry) {
	delete(c.entriesMap, entry.name)
	entry.previous.next = entry.next
	entry.next.previous = entry.previous
	entry.previous = nil
	entry.next = nil
	c.changeID++
}

func (c *inMemoryDirectoryContents) mayAttach(name path.Component) syscall.Errno {
	if c.isDeleted {
		return syscall.ENOENT
	}
	if _, ok := c.entriesMap[name]; ok {
		return syscall.EEXIST
	}
	return 0
}

func (c *inMemoryDirectoryContents) virtualMayAttach(name path.Component) Status {
	if c.isDeleted {
		return StatusErrNoEnt
	}
	if _, ok := c.entriesMap[name]; ok {
		return StatusErrExist
	}
	return StatusOK
}

func (c *inMemoryDirectoryContents) isEmpty() bool {
	return len(c.entriesMap) == 0
}

func (c *inMemoryDirectoryContents) createChildren(subtree *inMemorySubtree, children map[path.Component]InitialNode) {
	// Either sort or shuffle the children before inserting them
	// into the directory. This either makes VirtualReadDir() behave
	// deterministically, or not, based on preference.
	namesList := make(path.ComponentsList, 0, len(children))
	for name := range children {
		namesList = append(namesList, name)
	}
	subtree.filesystem.initialContentsSorter(namesList)

	for _, name := range namesList {
		if child := children[name]; child.Directory != nil {
			c.attachNewDirectory(subtree, name, child.Directory)
		} else {
			c.attach(name, inMemoryDirectoryChild{leaf: child.Leaf})
		}
	}
}

func (c *inMemoryDirectoryContents) getEntryAtCookie(firstCookie uint64) *inMemoryDirectoryEntry {
	entry := c.entriesList.next
	for {
		if entry == &c.entriesList || entry.cookie >= firstCookie {
			return entry
		}
		entry = entry.next
	}
}

// getAndLockIfDirectory obtains a child from the current directory, and
// immediately locks it if it is a directory. To prevent possible
// deadlocks, we must respect the lock order. This may require this
// function to drop the lock on current directories prior to picking up
// the lock of the child directory.
func (c *inMemoryDirectoryContents) getAndLockIfDirectory(name path.Component, lockPile *re_sync.LockPile) (*inMemoryDirectoryEntry, bool) {
	for {
		entry, ok := c.entriesMap[name]
		if !ok {
			// No child node present.
			return nil, false
		}
		if !entry.child.isDirectory() {
			// Not a directory.
			return entry, true
		}
		childDirectoryLock := &entry.child.directory.lock
		if lockPile.Lock(childDirectoryLock) {
			// Lock acquisition of child succeeded without
			// dropping any of the existing locks.
			return entry, true
		}
		if c.entriesMap[name] == entry {
			// Even though we dropped locks, no race occurred.
			return entry, true
		}
		lockPile.Unlock(childDirectoryLock)
	}
}

func (c *inMemoryDirectoryContents) getDirectoriesAndLeavesCount() (directoriesCount, leavesCount int) {
	for entry := c.entriesList.next; entry != &c.entriesList; entry = entry.next {
		if entry.child.isDirectory() {
			directoriesCount++
		} else {
			leavesCount++
		}
	}
	return
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
	subtree *inMemorySubtree
	handle  StatefulDirectoryHandle

	lock                   sync.Mutex
	initialContentsFetcher InitialContentsFetcher
	contents               inMemoryDirectoryContents
}

// NewInMemoryPrepopulatedDirectory creates a new PrepopulatedDirectory
// that keeps all directory metadata stored in memory. As the filesystem
// API does not allow traversing the hierarchy upwards, this directory
// can be considered the root directory of the hierarchy.
func NewInMemoryPrepopulatedDirectory(fileAllocator FileAllocator, symlinkFactory SymlinkFactory, errorLogger util.ErrorLogger, handleAllocator StatefulHandleAllocator, initialContentsSorter Sorter) PrepopulatedDirectory {
	subtree := &inMemorySubtree{
		filesystem: &inMemoryFilesystem{
			symlinkFactory:          symlinkFactory,
			statefulHandleAllocator: handleAllocator,
			initialContentsSorter:   initialContentsSorter,
		},
		fileAllocator: fileAllocator,
		errorLogger:   errorLogger,
	}
	return subtree.createNewDirectory(EmptyInitialContentsFetcher)
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
		i.contents.initialize()
		i.contents.createChildren(i.subtree, children)
	}
	return &i.contents, nil
}

func (i *inMemoryPrepopulatedDirectory) markDeleted() {
	if !i.contents.isDeleted {
		if i.initialContentsFetcher != nil || !i.contents.isEmpty() {
			panic("Attempted to delete a directory that was not empty")
		}
		i.contents.isDeleted = true
		i.handle.Release()
	}
}

func (i *inMemoryPrepopulatedDirectory) LookupChild(name path.Component) (PrepopulatedDirectory, NativeLeaf, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, err := i.getContents()
	if err != nil {
		return nil, nil, err
	}

	if entry, ok := contents.entriesMap[name]; ok {
		child := &entry.child
		if child.isDirectory() {
			return child.directory, nil, nil
		}
		return nil, child.leaf, nil
	}
	return nil, nil, syscall.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) LookupAllChildren() ([]DirectoryPrepopulatedDirEntry, []LeafPrepopulatedDirEntry, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, err := i.getContents()
	if err != nil {
		return nil, nil, err
	}

	directoriesCount, leavesCount := contents.getDirectoriesAndLeavesCount()
	directories := make(directoryPrepopulatedDirEntryList, 0, directoriesCount)
	leaves := make(leafPrepopulatedDirEntryList, 0, leavesCount)
	for entry := contents.entriesList.next; entry != &contents.entriesList; entry = entry.next {
		if child := &entry.child; child.isDirectory() {
			directories = append(directories, DirectoryPrepopulatedDirEntry{
				Child: child.directory,
				Name:  entry.name,
			})
		} else {
			leaves = append(leaves, LeafPrepopulatedDirEntry{
				Child: child.leaf,
				Name:  entry.name,
			})
		}
	}

	sort.Sort(directories)
	sort.Sort(leaves)
	return directories, leaves, nil
}

func (i *inMemoryPrepopulatedDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, err := i.getContents()
	if err != nil {
		return nil, err
	}

	entries := make(filesystem.FileInfoList, 0, len(contents.entriesMap))
	for entry := contents.entriesList.next; entry != &contents.entriesList; entry = entry.next {
		if child := &entry.child; child.isDirectory() {
			entries = append(entries,
				filesystem.NewFileInfo(entry.name, filesystem.FileTypeDirectory, false))
		} else {
			entries = append(entries, GetFileInfo(entry.name, child.leaf))
		}
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

	if entry, ok := contents.getAndLockIfDirectory(name, &lockPile); ok {
		if child := &entry.child; child.isDirectory() {
			// The directory has a child directory under
			// that name. Perform an rmdir().
			childContents, err := child.directory.getContents()
			if err != nil {
				return err
			}
			if !childContents.isEmpty() {
				return syscall.ENOTEMPTY
			}
			child.directory.markDeleted()
		} else {
			// The directory has a child file/symlink under
			// that name. Perform an unlink().
			child.leaf.Unlink()
		}
		contents.detach(entry)
		lockPile.UnlockAll()
		i.handle.NotifyRemoval(name)
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

	if entry, ok := contents.entriesMap[name]; ok {
		contents.detach(entry)
		i.lock.Unlock()
		i.handle.NotifyRemoval(name)
		if child := &entry.child; child.isDirectory() {
			// The directory has a child directory under
			// that name. Perform a recursive removal.
			child.directory.removeAllChildren(true)
		} else {
			// The directory has a child file/symlink under
			// that name. Perform an unlink().
			child.leaf.Unlink()
		}
		return nil
	}

	i.lock.Unlock()
	return syscall.ENOENT
}

func (i *inMemoryPrepopulatedDirectory) RemoveAllChildren(deleteSelf bool) error {
	i.removeAllChildren(deleteSelf)
	return nil
}

func (i *inMemoryPrepopulatedDirectory) removeAllChildren(deleteSelf bool) {
	i.lock.Lock()
	if i.initialContentsFetcher != nil {
		// The directory has not been initialized. Instead of
		// initializing it as intended and removing all
		// contents, forcefully initialize it as an empty
		// directory.
		i.initialContentsFetcher = nil
		i.contents.initialize()
		if deleteSelf {
			i.markDeleted()
		}
		i.lock.Unlock()
	} else {
		// Detach all contents from the directory.
		var entries *inMemoryDirectoryEntry
		for i.contents.entriesList.next != &i.contents.entriesList {
			entry := i.contents.entriesList.next
			i.contents.detach(entry)
			entry.previous = entries
			entries = entry
		}
		if deleteSelf {
			i.markDeleted()
		}
		i.lock.Unlock()

		i.postRemoveChildren(entries)
	}
}

// postRemoveChildren is called after bulk unlinking files and
// directories and dropping the parent directory lock. It invalidates
// all entries in the FUSE directory entry cache and recursively removes
// all files.
func (i *inMemoryPrepopulatedDirectory) postRemoveChildren(entries *inMemoryDirectoryEntry) {
	for entry := entries; entry != nil; entry = entry.previous {
		i.handle.NotifyRemoval(entry.name)
		if child := &entry.child; child.isDirectory() {
			child.directory.removeAllChildren(true)
		} else {
			child.leaf.Unlink()
		}
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

	if contents.isDeleted {
		i.lock.Unlock()
		return syscall.ENOENT
	}

	// Remove entries that are about to be overwritten.
	var overwrittenEntries *inMemoryDirectoryEntry
	if overwrite {
		for name := range children {
			if entry, ok := contents.entriesMap[name]; ok {
				contents.detach(entry)
				entry.previous = overwrittenEntries
				overwrittenEntries = entry
			}
		}
	} else {
		for name := range children {
			if _, ok := contents.entriesMap[name]; ok {
				i.lock.Unlock()
				return syscall.EEXIST
			}
		}
	}

	contents.createChildren(i.subtree, children)
	i.lock.Unlock()

	i.postRemoveChildren(overwrittenEntries)
	return nil
}

func (i *inMemoryPrepopulatedDirectory) CreateAndEnterPrepopulatedDirectory(name path.Component) (PrepopulatedDirectory, error) {
	i.lock.Lock()

	contents, err := i.getContents()
	if err != nil {
		i.lock.Unlock()
		return nil, err
	}

	if entry, ok := contents.entriesMap[name]; ok {
		child := &entry.child
		if child.isDirectory() {
			// Already a directory.
			i.lock.Unlock()
			return child.directory, nil
		}
		// Not a directory. Replace it.
		contents.detach(entry)
		child.leaf.Unlink()
		newChild := contents.attachNewDirectory(i.subtree, name, EmptyInitialContentsFetcher)
		i.lock.Unlock()
		i.handle.NotifyRemoval(name)
		return newChild, nil
	}

	if contents.isDeleted {
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
	directoriesCount, leavesCount := i.contents.getDirectoriesAndLeavesCount()
	directories := make([]*inMemoryPrepopulatedDirectory, 0, directoriesCount)
	leaves := make([]leafInfo, 0, leavesCount)
	for entry := i.contents.entriesList.next; entry != &i.contents.entriesList; entry = entry.next {
		if child := &entry.child; child.isDirectory() {
			directories = append(directories, child.directory)
		} else {
			leaves = append(leaves, leafInfo{
				name: entry.name,
				leaf: child.leaf,
			})
		}
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

func (i *inMemoryPrepopulatedDirectory) virtualGetContents() (*inMemoryDirectoryContents, Status) {
	contents, err := i.getContents()
	if err != nil {
		i.subtree.errorLogger.Log(util.StatusWrap(err, "Failed to initialize directory"))
		return nil, StatusErrIO
	}
	return contents, StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualOpenChild(name path.Component, shareAccess ShareMask, createAttributes *Attributes, existingOptions *OpenExistingOptions, requested AttributesMask, openedFileAttributes *Attributes) (Leaf, AttributesMask, ChangeInfo, Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return nil, 0, ChangeInfo{}, s
	}

	if entry, ok := contents.entriesMap[name]; ok {
		// File already exists.
		if existingOptions == nil {
			return nil, 0, ChangeInfo{}, StatusErrExist
		}
		child := &entry.child
		if child.isDirectory() {
			return nil, 0, ChangeInfo{}, StatusErrIsDir
		}
		leaf := child.leaf
		s := leaf.VirtualOpenSelf(shareAccess, existingOptions, requested, openedFileAttributes)
		return leaf, existingOptions.ToAttributesMask(), ChangeInfo{
			Before: contents.changeID,
			After:  contents.changeID,
		}, s
	}

	// File doesn't exist.
	if contents.isDeleted || createAttributes == nil {
		return nil, 0, ChangeInfo{}, StatusErrNoEnt
	}

	// Create new file with attributes provided.
	var respected AttributesMask
	isExecutable := false
	if permissions, ok := createAttributes.GetPermissions(); ok {
		respected |= AttributesMaskPermissions
		isExecutable = permissions&PermissionsExecute != 0
	}
	size := uint64(0)
	if sizeBytes, ok := createAttributes.GetSizeBytes(); ok {
		respected |= AttributesMaskSizeBytes
		size = sizeBytes
	}
	leaf, s := i.subtree.fileAllocator.NewFile(isExecutable, size)
	if s != StatusOK {
		return nil, 0, ChangeInfo{}, s
	}

	// Attach file to the directory.
	changeIDBefore := contents.changeID
	contents.attach(name, inMemoryDirectoryChild{leaf: leaf})
	leaf.VirtualGetAttributes(requested, openedFileAttributes)
	return leaf, respected, ChangeInfo{
		Before: changeIDBefore,
		After:  contents.changeID,
	}, StatusOK
}

const inMemoryPrepopulatedDirectoryLockedAttributesMask = AttributesMaskChangeID

func (i *inMemoryPrepopulatedDirectory) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	i.virtualGetAttributesUnlocked(requested, attributes)
	if requested&inMemoryPrepopulatedDirectoryLockedAttributesMask != 0 {
		i.lock.Lock()
		i.virtualGetAttributesLocked(requested, attributes)
		i.lock.Unlock()
	}
}

func (i *inMemoryPrepopulatedDirectory) virtualGetAttributesUnlocked(requested AttributesMask, attributes *Attributes) {
	attributes.SetFileType(filesystem.FileTypeDirectory)
	// To be consistent with traditional UNIX file systems, this
	// would need to be 2 + len(i.directories), but that would
	// require us to initialize the directory, which is undesirable.
	attributes.SetLinkCount(ImplicitDirectoryLinkCount)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite | PermissionsExecute)
	attributes.SetSizeBytes(0)
	i.handle.GetAttributes(requested, attributes)
}

func (i *inMemoryPrepopulatedDirectory) virtualGetAttributesLocked(requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(i.contents.changeID)
}

func (i *inMemoryPrepopulatedDirectory) VirtualLink(name path.Component, leaf Leaf, requested AttributesMask, out *Attributes) (ChangeInfo, Status) {
	child, ok := leaf.(NativeLeaf)
	if !ok {
		// The file is not the kind that can be embedded into
		// inMemoryPrepopulatedDirectory.
		return ChangeInfo{}, StatusErrXDev
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return ChangeInfo{}, s
	}

	if s := contents.virtualMayAttach(name); s != StatusOK {
		return ChangeInfo{}, s
	}
	if s := child.Link(); s != StatusOK {
		return ChangeInfo{}, s
	}
	changeIDBefore := contents.changeID
	contents.attach(name, inMemoryDirectoryChild{leaf: child})

	child.VirtualGetAttributes(requested, out)
	return ChangeInfo{
		Before: changeIDBefore,
		After:  contents.changeID,
	}, StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualLookup(name path.Component, requested AttributesMask, out *Attributes) (Directory, Leaf, Status) {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return nil, nil, s
	}

	// Depending on which attributes need to be returned, we either
	// need to lock the child directory or not. We can't just call
	// into VirtualGetAttributes() on the child directory, as that
	// might cause a deadlock.
	if requested&inMemoryPrepopulatedDirectoryLockedAttributesMask != 0 {
		if entry, ok := contents.getAndLockIfDirectory(name, &lockPile); ok {
			child := &entry.child
			if child.isDirectory() {
				child.directory.virtualGetAttributesUnlocked(requested, out)
				child.directory.virtualGetAttributesLocked(requested, out)
				return child.directory, nil, StatusOK
			}
			child.leaf.VirtualGetAttributes(requested, out)
			return nil, child.leaf, StatusOK
		}
	} else {
		if entry, ok := contents.entriesMap[name]; ok {
			child := &entry.child
			if child.isDirectory() {
				child.directory.virtualGetAttributesUnlocked(requested, out)
				return child.directory, nil, StatusOK
			}
			child.leaf.VirtualGetAttributes(requested, out)
			return nil, child.leaf, StatusOK
		}
	}
	return nil, nil, StatusErrNoEnt
}

func (i *inMemoryPrepopulatedDirectory) VirtualMkdir(name path.Component, requested AttributesMask, out *Attributes) (Directory, ChangeInfo, Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return nil, ChangeInfo{}, s
	}

	if s := contents.virtualMayAttach(name); s != StatusOK {
		return nil, ChangeInfo{}, s
	}
	changeIDBefore := contents.changeID
	child := contents.attachNewDirectory(i.subtree, name, EmptyInitialContentsFetcher)

	// Even though the child directory is not locked explicitly, the
	// following is safe, as the directory has not been returned yet.
	child.virtualGetAttributesUnlocked(requested, out)
	child.virtualGetAttributesLocked(requested, out)
	return child, ChangeInfo{
		Before: changeIDBefore,
		After:  contents.changeID,
	}, StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualMknod(name path.Component, fileType filesystem.FileType, requested AttributesMask, out *Attributes) (Leaf, ChangeInfo, Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return nil, ChangeInfo{}, s
	}

	if s := contents.virtualMayAttach(name); s != StatusOK {
		return nil, ChangeInfo{}, s
	}
	// Every FIFO or UNIX domain socket needs to have its own inode
	// number, as the kernel uses that to tell instances apart. We
	// therefore consider it to be stateful, like a writable file.
	child := i.subtree.filesystem.statefulHandleAllocator.
		New().
		AsNativeLeaf(NewSpecialFile(fileType, nil))
	changeIDBefore := contents.changeID
	contents.attach(name, inMemoryDirectoryChild{leaf: child})

	child.VirtualGetAttributes(requested, out)
	return child, ChangeInfo{
		Before: changeIDBefore,
		After:  contents.changeID,
	}, StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualReadDir(firstCookie uint64, requested AttributesMask, reporter DirectoryEntryReporter) Status {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return s
	}

	for entry := contents.getEntryAtCookie(firstCookie); entry != &contents.entriesList; {
		if child := &entry.child; child.isDirectory() {
			directory := child.directory
			var attributes Attributes
			directory.virtualGetAttributesUnlocked(requested, &attributes)

			// The caller requested attributes that can only
			// be obtained by locking the child directory.
			// This may require us to briefly drop the lock
			// on the parent directory, which may invalidate
			// the current directory entry.
			//
			// Because we clear directory entries while
			// detaching, we can detect this and retry by
			// seeking through the directory once again.
			if requested&inMemoryPrepopulatedDirectoryLockedAttributesMask != 0 {
				if !lockPile.Lock(&directory.lock) && entry.next == nil {
					lockPile.Unlock(&directory.lock)
					entry = contents.getEntryAtCookie(entry.cookie)
					continue
				}
				directory.virtualGetAttributesLocked(requested, &attributes)
				lockPile.Unlock(&directory.lock)
			}

			if !reporter.ReportDirectory(entry.cookie+1, entry.name, directory, &attributes) {
				break
			}
		} else {
			leaf := child.leaf
			var attributes Attributes
			leaf.VirtualGetAttributes(requested, &attributes)
			if !reporter.ReportLeaf(entry.cookie+1, entry.name, leaf, &attributes) {
				break
			}
		}
		entry = entry.next
	}
	return StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualRename(oldName path.Component, newDirectory Directory, newName path.Component) (ChangeInfo, ChangeInfo, Status) {
	iOld := i
	iNew, ok := newDirectory.(*inMemoryPrepopulatedDirectory)
	if !ok {
		return ChangeInfo{}, ChangeInfo{}, StatusErrXDev
	}

	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&iOld.lock, &iNew.lock)

	oldContents, s := iOld.virtualGetContents()
	if s != StatusOK {
		return ChangeInfo{}, ChangeInfo{}, s
	}
	newContents, s := iNew.virtualGetContents()
	if s != StatusOK {
		return ChangeInfo{}, ChangeInfo{}, s
	}

	oldChangeIDBefore := oldContents.changeID
	newChangeIDBefore := newContents.changeID
	if newEntry, ok := newContents.getAndLockIfDirectory(newName, &lockPile); ok {
		oldEntry, ok := oldContents.entriesMap[oldName]
		if !ok {
			return ChangeInfo{}, ChangeInfo{}, StatusErrNoEnt
		}
		oldChild := oldEntry.child
		if newChild := &newEntry.child; newChild.isDirectory() {
			// Renaming to a location at which a directory
			// already exists.
			if !oldChild.isDirectory() {
				return ChangeInfo{}, ChangeInfo{}, StatusErrIsDir
			}
			// Renaming a directory to itself is always
			// permitted, even when not empty.
			if newChild.directory != oldChild.directory {
				if iOld.subtree.filesystem != iNew.subtree.filesystem {
					return ChangeInfo{}, ChangeInfo{}, StatusErrXDev
				}
				newChildContents, s := newChild.directory.virtualGetContents()
				if s != StatusOK {
					return ChangeInfo{}, ChangeInfo{}, s
				}
				if !newChildContents.isEmpty() {
					return ChangeInfo{}, ChangeInfo{}, StatusErrNotEmpty
				}
				oldContents.detach(oldEntry)
				// TODO: Pick up an interlock and check for
				// potential creation of cyclic directory
				// structures.
				newContents.detach(newEntry)
				newChild.directory.markDeleted()
				newContents.attach(newName, oldChild)
			}
		} else {
			// Renaming to a location at which a leaf
			// already exists.
			if oldChild.isDirectory() {
				return ChangeInfo{}, ChangeInfo{}, StatusErrNotDir
			}
			// POSIX requires that renaming a file to itself
			// has no effect. After running the following
			// sequence of commands, both "a" and "b" should
			// still exist: "touch a; ln a b; mv a b".
			if newChild.leaf != oldChild.leaf {
				oldContents.detach(oldEntry)
				newContents.detach(newEntry)
				newChild.leaf.Unlink()
				newContents.attach(newName, oldChild)
			}
		}
	} else {
		// Renaming to a location where no file exists.
		if newContents.isDeleted {
			return ChangeInfo{}, ChangeInfo{}, StatusErrNoEnt
		}
		oldEntry, ok := oldContents.entriesMap[oldName]
		if !ok {
			return ChangeInfo{}, ChangeInfo{}, StatusErrNoEnt
		}
		oldChild := oldEntry.child
		if oldChild.isDirectory() {
			if iOld.subtree.filesystem != iNew.subtree.filesystem {
				return ChangeInfo{}, ChangeInfo{}, StatusErrXDev
			}
		}
		oldContents.detach(oldEntry)
		newContents.attach(newName, oldChild)
	}
	return ChangeInfo{
			Before: oldChangeIDBefore,
			After:  oldContents.changeID,
		}, ChangeInfo{
			Before: newChangeIDBefore,
			After:  newContents.changeID,
		}, StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualRemove(name path.Component, removeDirectory, removeLeaf bool) (ChangeInfo, Status) {
	lockPile := re_sync.LockPile{}
	defer lockPile.UnlockAll()
	lockPile.Lock(&i.lock)

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return ChangeInfo{}, s
	}

	if entry, ok := contents.getAndLockIfDirectory(name, &lockPile); ok {
		if child := &entry.child; child.isDirectory() {
			if !removeDirectory {
				return ChangeInfo{}, StatusErrPerm
			}
			childContents, s := child.directory.virtualGetContents()
			if s != StatusOK {
				return ChangeInfo{}, s
			}
			if !childContents.isEmpty() {
				return ChangeInfo{}, StatusErrNotEmpty
			}
			child.directory.markDeleted()
		} else {
			if !removeLeaf {
				return ChangeInfo{}, StatusErrNotDir
			}
			child.leaf.Unlink()
		}
		changeIDBefore := contents.changeID
		contents.detach(entry)
		return ChangeInfo{
			Before: changeIDBefore,
			After:  contents.changeID,
		}, StatusOK
	}

	return ChangeInfo{}, StatusErrNoEnt
}

func (i *inMemoryPrepopulatedDirectory) VirtualSetAttributes(in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	i.VirtualGetAttributes(requested, out)
	return StatusOK
}

func (i *inMemoryPrepopulatedDirectory) VirtualSymlink(pointedTo []byte, linkName path.Component, requested AttributesMask, out *Attributes) (Leaf, ChangeInfo, Status) {
	i.lock.Lock()
	defer i.lock.Unlock()

	contents, s := i.virtualGetContents()
	if s != StatusOK {
		return nil, ChangeInfo{}, s
	}

	if s := contents.virtualMayAttach(linkName); s != StatusOK {
		return nil, ChangeInfo{}, s
	}
	child := i.subtree.filesystem.symlinkFactory.LookupSymlink(pointedTo)
	changeIDBefore := contents.changeID
	contents.attach(linkName, inMemoryDirectoryChild{leaf: child})

	child.VirtualGetAttributes(requested, out)
	return child, ChangeInfo{
		Before: changeIDBefore,
		After:  contents.changeID,
	}, StatusOK
}

// directoryPrepopulatedDirEntryList is a list of DirectoryDirEntry
// objects returned by LookupAllChildren(). This type may be used to
// sort elements in the list by name.
type directoryPrepopulatedDirEntryList []DirectoryPrepopulatedDirEntry

func (l directoryPrepopulatedDirEntryList) Len() int {
	return len(l)
}

func (l directoryPrepopulatedDirEntryList) Less(i, j int) bool {
	return l[i].Name.String() < l[j].Name.String()
}

func (l directoryPrepopulatedDirEntryList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// leafPrepopulatedDirEntryList is a list of LeafPrepopulatedDirEntry
// objects returned by LookupAllChildren(). This type may be used to
// sort elements in the list by name.
type leafPrepopulatedDirEntryList []LeafPrepopulatedDirEntry

func (l leafPrepopulatedDirEntryList) Len() int {
	return len(l)
}

func (l leafPrepopulatedDirEntryList) Less(i, j int) bool {
	return l[i].Name.String() < l[j].Name.String()
}

func (l leafPrepopulatedDirEntryList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
