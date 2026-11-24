package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// FileFetcher is responsible for fetching files from the Content
// Addressable Storage (CAS), storing its contents inside a file on
// disk.
type FileFetcher interface {
	GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error

	// CheckReadiness returns an error if GetFile() is known to be
	// unable to place files in the provided directory, without
	// fetching anything from the CAS. This allows a worker to stop
	// accepting build actions that it would fail.
	CheckReadiness(ctx context.Context, directory filesystem.Directory) error
}
