// +build darwin linux

package fuse

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// EntryNotifier is a callback that a hierarchy of Directory objects may
// use to report that directory entries have been removed. This causes
// them to be removed from the directory entry cache used by FUSE as
// well.
//
// It is advised that this callback is invoked without holding any other
// locks, as it writes to the FUSE character device, which may block.
type EntryNotifier func(parent uint64, name string) fuse.Status

// DirectoryDirEntry contains information about a directory node that is
// returned by FUSEReadDirPlus().
type DirectoryDirEntry struct {
	Child    Directory
	DirEntry fuse.DirEntry
}

// LeafDirEntry contains information about a leaf node that is returned
// by FUSEReadDirPlus().
type LeafDirEntry struct {
	Child    Leaf
	DirEntry fuse.DirEntry
}

// Directory node that is exposed through FUSE using
// SimpleRawFileSystem. The names of all of these operations are
// prefixed with 'FUSE' to ensure they don't collide with
// filesystem.Directory.
type Directory interface {
	node

	FUSECreate(name string, flags, mode uint32, out *fuse.Attr) (Leaf, fuse.Status)
	FUSELink(name string, leaf Leaf, out *fuse.Attr) fuse.Status
	FUSELookup(name string, out *fuse.Attr) (Directory, Leaf, fuse.Status)
	FUSEMkdir(name string, mode uint32, out *fuse.Attr) (Directory, fuse.Status)
	FUSEMknod(name string, mode, dev uint32, out *fuse.Attr) (Leaf, fuse.Status)
	FUSEReadDir() ([]fuse.DirEntry, fuse.Status)
	FUSEReadDirPlus() ([]DirectoryDirEntry, []LeafDirEntry, fuse.Status)
	FUSERename(oldName string, newDirectory Directory, newName string) fuse.Status
	FUSERmdir(name string) fuse.Status
	FUSESymlink(pointedTo, linkName string, out *fuse.Attr) (Leaf, fuse.Status)
	FUSEUnlink(name string) fuse.Status
}
