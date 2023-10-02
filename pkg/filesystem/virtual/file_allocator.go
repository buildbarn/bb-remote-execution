package virtual

// FileAllocator is called into by InMemoryPrepopulatedDirectory to
// create new files within the file system. Such files could either be
// stored in memory, on disk, remotely, etc.
//
// Files returned by this interface should have a link count of 1, and
// are opened using the provided share access mask.
type FileAllocator interface {
	NewFile(isExecutable bool, size uint64, shareAccess ShareMask) (NativeLeaf, Status)
}
