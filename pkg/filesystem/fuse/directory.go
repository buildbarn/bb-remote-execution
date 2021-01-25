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
type EntryNotifier func(parent uint64, name path.Component)

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

	// FUSECreate creates a regular file within the directory.
	FUSECreate(name path.Component, flags, mode uint32, out *fuse.Attr) (Leaf, fuse.Status)
	// FUSELink links an existing file into the directory.
	FUSELink(name path.Component, leaf Leaf, out *fuse.Attr) fuse.Status
	// FUSELookup obtains the inode corresponding with a child
	// stored within the directory.
	FUSELookup(name path.Component, out *fuse.Attr) (Directory, Leaf, fuse.Status)
	// FUSEMkdir creates an empty directory within the current
	// directory.
	FUSEMkdir(name path.Component, mode uint32, out *fuse.Attr) (Directory, fuse.Status)
	// FUSEMknod creates a character device, block device, FIFO or
	// UNIX domain socket within the current directory.
	FUSEMknod(name path.Component, mode, dev uint32, out *fuse.Attr) (Leaf, fuse.Status)
	// FUSEReadDir returns a list of all files and directories
	// stored within the directory.
	FUSEReadDir() ([]fuse.DirEntry, fuse.Status)
	// FUSEReadDirPlus returns a list of all files and directories
	// stored within the directory. In addition to that, it returns
	// handles to each of the entries, so that there is no need to
	// call FUSELookup on them separately.
	FUSEReadDirPlus() ([]DirectoryDirEntry, []LeafDirEntry, fuse.Status)
	// FUSERename renames a file stored in the current directory,
	// potentially moving it to another directory.
	FUSERename(oldName path.Component, newDirectory Directory, newName path.Component) fuse.Status
	// FUSERmdir removes an empty directory stored within the
	// current directory.
	FUSERmdir(name path.Component) fuse.Status
	// FUSESymlink creates a symbolic link within the current
	// directory.
	FUSESymlink(pointedTo string, linkName path.Component, out *fuse.Attr) (Leaf, fuse.Status)
	// FUSEUnlink removes a leaf node from the current directory.
	FUSEUnlink(name path.Component) fuse.Status
}

const (
	// ImplicitDirectoryLinkCount is the value that should be
	// assigned to fuse.attr.Nlink for directory nodes for which the
	// directory contents are not defined explicitly. These may be
	// directories that are lazy-loading, or have an infinite number
	// of children due to them being defined programmatically.
	//
	// It is important that we return a link count lower than two
	// for these directories. Tools like GNU find(1) rely on an
	// accurate link count to rule out the existence of child
	// directories. A link count below two instructs them to disable
	// such optimizations, forcing them to read directory listings.
	// See the "-noleaf" option in find(1)'s man page for details.
	//
	// File systems such as btrfs also set the link count of
	// directories to one.
	ImplicitDirectoryLinkCount = 1
	// EmptyDirectoryLinkCount is the value that should be assigned
	// to fuse.attr.Nlink for directory nodes that do not have any
	// child directories.
	EmptyDirectoryLinkCount = 2
)
