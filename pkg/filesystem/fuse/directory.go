// +build darwin linux

package fuse

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// EntryNotifier is a callback that a hierarchy of Directory objects may
// use to report that directory entries have been removed. This causes
// them to be removed from the directory entry cache used by FUSE as
// well.
//
// It is advised that this callback is invoked without holding any other
// locks, as it writes to the FUSE character device, which may block.
type EntryNotifier func(parent uint64, name path.Component) fuse.Status

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

	FUSECreate(name path.Component, flags, mode uint32, out *fuse.Attr) (Leaf, fuse.Status)
	FUSELink(name path.Component, leaf Leaf, out *fuse.Attr) fuse.Status
	FUSELookup(name path.Component, out *fuse.Attr) (Directory, Leaf, fuse.Status)
	FUSEMkdir(name path.Component, mode uint32, out *fuse.Attr) (Directory, fuse.Status)
	FUSEMknod(name path.Component, mode, dev uint32, out *fuse.Attr) (Leaf, fuse.Status)
	FUSEReadDir() ([]fuse.DirEntry, fuse.Status)
	FUSEReadDirPlus() ([]DirectoryDirEntry, []LeafDirEntry, fuse.Status)
	FUSERename(oldName path.Component, newDirectory Directory, newName path.Component) fuse.Status
	FUSERmdir(name path.Component) fuse.Status
	FUSESymlink(pointedTo string, linkName path.Component, out *fuse.Attr) (Leaf, fuse.Status)
	FUSEUnlink(name path.Component) fuse.Status
}
