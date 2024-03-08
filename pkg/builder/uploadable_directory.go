package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// UploadableDirectory is a directory that can be uploaded into the
// Content Addressable Storage. It is provided to
// OutputHierarchy.UploadOutputs(), which traverses it and uploads paths
// that were specified in the Action message.
type UploadableDirectory interface {
	// Methods inherited from filesystem.Directory.
	Close() error
	EnterUploadableDirectory(name path.Component) (UploadableDirectory, error)
	Lstat(name path.Component) (filesystem.FileInfo, error)
	ReadDir() ([]filesystem.FileInfo, error)
	Readlink(name path.Component) (path.Parser, error)

	// Upload a file into the Content Addressable Storage.
	//
	// Implementations that are capable of detecting that files
	// remain opened for writing may block until they are closed. To
	// ensure this does not end up blocking indefinitely, a channel
	// is provided that gets closed after a configured amount of
	// time. This channel is also closed when the Context is done.
	UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error)
}
