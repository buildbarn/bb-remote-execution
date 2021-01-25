// +build darwin linux

package fuse

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// PrepopulatedDirectory is a Directory that is writable and can contain
// files of type NativeLeaf.
//
// By making use of InitialContentsFetcher, it is possible to create
// subdirectories that are prepopulated with files and directories.
// These will be instantiated only when accessed. This feature is used
// by bb_worker to lazily load the input root while a build action is
// being executed. Similarly, it is used by bb_clientd to lazily
// instantiate the contents of a Tree object.
type PrepopulatedDirectory interface {
	Directory

	// LookupChild() looks up a file or directory contained in a
	// PrepopulatedDirectory. This method is similar to
	// FUSELookup(), except that it returns the native types managed
	// by PrepopulatedDirectory.
	LookupChild(name path.Component) (PrepopulatedDirectory, NativeLeaf, error)
	// CreateChildren() creates one or more files or directories in
	// the current directory.
	//
	// If the overwrite flag is set, existing files and directories
	// will be replaced. If the overwrite flag is not set, the call
	// will fail if one or more entries already exist. No changes
	// will be made to the directory in that case.
	CreateChildren(children map[path.Component]InitialNode, overwrite bool) error
	// CreateAndEnterPrepopulatedDirectory() is similar to
	// LookupChild(), except that it creates the specified directory
	// if it does not yet exist. If a file already exists, it will
	// be removed.
	CreateAndEnterPrepopulatedDirectory(name path.Component) (PrepopulatedDirectory, error)
	// RemoveAllChildren() removes all files and directories
	// contained in the current directory.
	//
	// This method is identical to the one filesystem.Directory,
	// except that the forbidNewChildren flag may be set to
	// permanently mark the directory in such a way that no further
	// files may be added. When called on the root directory, all
	// resources associated with the directory hierarchy will be
	// released.
	RemoveAllChildren(forbidNewChildren bool) error
	// InstallHooks sets up hooks for creating files and logging
	// errors that occur under the directory subtree.
	//
	// This function is identical to BuildDirectory.InstallHooks(),
	// except that it uses the FUSE specific FileAllocator instead
	// of FilePool.
	InstallHooks(fileAllocator FileAllocator, errorLogger util.ErrorLogger)

	// Functions inherited from filesystem.Directory.
	ReadDir() ([]filesystem.FileInfo, error)
	RemoveAll(name path.Component) error
	Remove(name path.Component) error
}
