package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// DirectoryEntryReporter is used by VirtualReadDir() to report
// individual directory entries. These methods may be called while locks
// on the underlying directory are held. This means that it's not safe
// to call methods of the child directory, as that could cause
// deadlocks.
type DirectoryEntryReporter interface {
	// TODO: Can't use DirectoryChild in the arguments here, due to
	// https://github.com/golang/go/issues/50259.
	ReportEntry(nextCookie uint64, name path.Component, child Child[Directory, Leaf, Node], attributes *Attributes) bool
}

// ChangeInfo contains a pair of change IDs of a directory, before and
// after performing a directory mutating operation. This information
// needs to be returned by various NFSv4 operations.
type ChangeInfo struct {
	Before uint64
	After  uint64
}

// DirectoryChild is either a Directory or a Leaf, as returned by
// Directory.VirtualLookup().
type DirectoryChild = Child[Directory, Leaf, Node]

// Directory node that is exposed through FUSE using
// SimpleRawFileSystem, or through NFSv4. The names of all of these
// operations are prefixed with 'Virtual' to ensure they don't collide
// with filesystem.Directory.
type Directory interface {
	Node

	// VirtualOpenChild opens a regular file within the directory.
	//
	// When createAttributes is nil, this method will fail with
	// StatusErrNoEnt if the file does not exist. When not nil, a
	// file will be created.
	//
	// When existingOptions is nil, this method will fail with
	// StatusErrExist if the file already exists. When not nil, an
	// existing file will be opened.
	//
	// Either one or both of createAttributes and existingOptions
	// need to be provided.
	VirtualOpenChild(ctx context.Context, name path.Component, shareAccess ShareMask, createAttributes *Attributes, existingOptions *OpenExistingOptions, requested AttributesMask, openedFileAttributes *Attributes) (Leaf, AttributesMask, ChangeInfo, Status)
	// VirtualLink links an existing file into the directory.
	VirtualLink(ctx context.Context, name path.Component, leaf Leaf, requested AttributesMask, attributes *Attributes) (ChangeInfo, Status)
	// VirtualLookup obtains the inode corresponding with a child
	// stored within the directory.
	//
	// TODO: Can't use DirectoryChild in the return type here, due to
	// https://github.com/golang/go/issues/50259.
	VirtualLookup(ctx context.Context, name path.Component, requested AttributesMask, out *Attributes) (Child[Directory, Leaf, Node], Status)
	// VirtualMkdir creates an empty directory within the current
	// directory.
	VirtualMkdir(name path.Component, requested AttributesMask, attributes *Attributes) (Directory, ChangeInfo, Status)
	// VirtualMknod creates a character FIFO or UNIX domain socket
	// within the current directory.
	VirtualMknod(ctx context.Context, name path.Component, fileType filesystem.FileType, requested AttributesMask, attributes *Attributes) (Leaf, ChangeInfo, Status)
	// VirtualReadDir reports files and directories stored within
	// the directory.
	VirtualReadDir(ctx context.Context, firstCookie uint64, requested AttributesMask, reporter DirectoryEntryReporter) Status
	// VirtualRename renames a file stored in the current directory,
	// potentially moving it to another directory.
	VirtualRename(oldName path.Component, newDirectory Directory, newName path.Component) (ChangeInfo, ChangeInfo, Status)
	// VirtualRemove removes an empty directory or leaf node stored
	// within the current directory. Depending on the parameters,
	// this method behaves like rmdir(), unlink() or a mixture of
	// the two. The latter is needed by NFSv4.
	VirtualRemove(name path.Component, removeDirectory, removeLeaf bool) (ChangeInfo, Status)
	// VirtualSymlink creates a symbolic link within the current
	// directory.
	VirtualSymlink(ctx context.Context, pointedTo []byte, linkName path.Component, requested AttributesMask, attributes *Attributes) (Leaf, ChangeInfo, Status)
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
	ImplicitDirectoryLinkCount uint32 = 1
	// EmptyDirectoryLinkCount is the value that should be assigned
	// to fuse.attr.Nlink for directory nodes that do not have any
	// child directories.
	EmptyDirectoryLinkCount uint32 = 2
)
