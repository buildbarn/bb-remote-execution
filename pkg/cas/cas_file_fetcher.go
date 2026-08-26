package cas

import (
	"context"
	"os"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type blobAccessFileFetcher struct {
	contentAddressableStorage cas.ContentAddressableStorage
}

// NewCASFileFetcher creates a FileFetcher that reads files fom a
// Content Addressable Storage (CAS).
func NewCASFileFetcher(contentAddressableStorage cas.ContentAddressableStorage) FileFetcher {
	return &blobAccessFileFetcher{
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (ff *blobAccessFileFetcher) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
	var mode os.FileMode = 0o444
	if isExecutable {
		mode = 0o555
	}

	w, err := directory.OpenAppend(name, filesystem.CreateExcl(mode))
	if err != nil {
		return err
	}
	defer w.Close()

	if err := cas.IntoWriter(ctx, ff.contentAddressableStorage, digest, 0, w); err != nil {
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
