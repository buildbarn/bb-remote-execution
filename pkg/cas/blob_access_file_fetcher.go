package cas

import (
	"context"
	"os"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type blobAccessFileFetcher struct {
	blobAccess blobstore.BlobAccess
}

// NewBlobAccessFileFetcher creates a FileFetcher that reads files fom a
// BlobAccess based store.
func NewBlobAccessFileFetcher(blobAccess blobstore.BlobAccess) FileFetcher {
	return &blobAccessFileFetcher{
		blobAccess: blobAccess,
	}
}

func (ff *blobAccessFileFetcher) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
	var mode os.FileMode = 0444
	if isExecutable {
		mode = 0555
	}

	w, err := directory.OpenAppend(name, filesystem.CreateExcl(mode))
	if err != nil {
		return err
	}
	defer w.Close()

	if err := ff.blobAccess.Get(ctx, digest).IntoWriter(w); err != nil {
		// Ensure no traces are left behind upon failure.
		directory.Remove(name)
		return err
	}
	time := filesystem.DeterministicFileModificationTimestamp
	if err := directory.Chtimes(name, time, time); err != nil {
		directory.Remove(name)
		return err
	}
	return nil
}
