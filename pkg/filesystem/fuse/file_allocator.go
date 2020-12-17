// +build darwin linux

package fuse

import (
	"os"
)

// FileAllocator is called into by InMemoryDirectory to create new files
// within the file system. Such files could either be stored in memory,
// on disk, remotely, etc.
type FileAllocator interface {
	NewFile(inodeNumber uint64, perm os.FileMode) (NativeLeaf, error)
}
