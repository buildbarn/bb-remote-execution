package builder

import (
	"context"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// BuildDirectory is a filesystem.Directory that may be used for the
// purpose of running build actions. BuildDirectory has a couple of
// special operations that implementations may use to run actions in a
// more efficient and manageable way.
type BuildDirectory interface {
	filesystem.DirectoryCloser

	// Identical to EnterDirectory(), except that it returns a
	// BuildDirectory object.
	EnterBuildDirectory(name string) (BuildDirectory, error)

	// Installs a set of hooks into the directory to intercept I/O
	// operations. The FilePool may be used to allocate storage
	// space. The ErrorLogger may be used to report fatal I/O
	// errors. Implementations of BuildDirectory are free to let
	// this be a no-op, with the disadvantage that they cannot apply
	// resource limits or provide rich I/O error messages.
	InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger)

	// Recursively merges the contents of a Directory stored in the
	// Content Addressable Storage into a local directory. If this
	// process is synchronous, this function can return a
	// synchronous error. If this process is lazy/asynchronous, the
	// provided ErrorLogger may be used to return an error.
	MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error
}
