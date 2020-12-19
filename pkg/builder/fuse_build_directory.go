// +build darwin linux

package builder

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type fuseBuildDirectory struct {
	*fuse.InMemoryDirectory

	directoryFetcher          cas.DirectoryFetcher
	contentAddressableStorage blobstore.BlobAccess
}

// NewFUSEBuildDirectory creates a BuildDirectory that is backed by a
// fuse.InMemoryDirectory. Instead of creating all files in the input
// root explicitly, it calls InMemoryDirectory.MergeDirectoryContents to
// add special file and directory nodes, whose contents are read on
// demand.
func NewFUSEBuildDirectory(directory *fuse.InMemoryDirectory, directoryFetcher cas.DirectoryFetcher, contentAddressableStorage blobstore.BlobAccess) BuildDirectory {
	return &fuseBuildDirectory{
		InMemoryDirectory:         directory,
		directoryFetcher:          directoryFetcher,
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (d *fuseBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	child, err := d.EnterInMemoryDirectory(name)
	if err != nil {
		return nil, err
	}
	return &fuseBuildDirectory{
		InMemoryDirectory:         child,
		directoryFetcher:          d.directoryFetcher,
		contentAddressableStorage: d.contentAddressableStorage,
	}, nil
}

func (d *fuseBuildDirectory) Close() error {
	// FUSE directories do not need to be released explicitly.
	return nil
}

func (d *fuseBuildDirectory) EnterUploadableDirectory(name path.Component) (UploadableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *fuseBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	d.InMemoryDirectory.InstallHooks(
		fuse.NewPoolBackedFileAllocator(filePool, errorLogger),
		errorLogger)
}

func (d *fuseBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error {
	initialContentsFetcher := fuse.NewContentAddressableStorageInitialContentsFetcher(
		ctx,
		d.directoryFetcher,
		d.contentAddressableStorage,
		errorLogger,
		digest)
	directories, leaves, err := initialContentsFetcher.FetchContents()
	if err != nil {
		return err
	}
	return d.InMemoryDirectory.MergeDirectoryContents(directories, leaves)
}

func (d *fuseBuildDirectory) UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function) (digest.Digest, error) {
	return d.InMemoryDirectory.UploadFile(ctx, name, d.contentAddressableStorage, digestFunction)
}
