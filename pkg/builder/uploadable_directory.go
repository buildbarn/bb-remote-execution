package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// UploadableDirectory is a directory that can be uploaded into the
// Content Addressable Storage. It is provided to
// OutputHierarchy.UploadOutputs(), which traverses it and uploads paths
// that were specified in the Action message.
type UploadableDirectory interface {
	// Methods inherited from filesystem.Directory.
	Close() error
	EnterUploadableDirectory(name string) (UploadableDirectory, error)
	Lstat(name string) (filesystem.FileInfo, error)
	ReadDir() ([]filesystem.FileInfo, error)
	Readlink(name string) (string, error)

	// Upload a file into the Content Addressable Storage.
	UploadFile(ctx context.Context, name string, digestFunction digest.Function) (digest.Digest, error)
}
