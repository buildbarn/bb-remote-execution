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
	Readlink(name path.Component) (string, error)

	// Upload a file into the Content Addressable Storage.
	UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function) (digest.Digest, error)
}
