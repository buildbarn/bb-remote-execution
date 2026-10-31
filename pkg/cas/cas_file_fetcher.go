package cas

import (
	"context"
	"os"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type blobAccessFileFetcher struct {
	readInfo cas.ReadInfo
}

// NewCASFileFetcher creates a FileFetcher that reads files fom a
// Content Addressable Storage (CAS).
func NewCASFileFetcher(readInfo cas.ReadInfo) FileFetcher {
	return &blobAccessFileFetcher{
		readInfo: readInfo,
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

	params, err := ff.readInfo.CDCParametersFetcher.FetchCDCParameters(ctx, digest.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Failed to fetch CDC parameters")
	}
	if err := cas.IntoWriter(ctx, ff.readInfo.ChunkBytesReader, ff.readInfo.ChunkMappingFetcher, params, digest, 0, w); err != nil {
		// Ensure no traces are left behind upon failure.
		directory.Remove(name)
		return FailedPreconditionOnMissingBlob(digest, err)
	}
	time := filesystem.DeterministicFileModificationTimestamp
	if err := directory.Chtimes(name, time, time); err != nil {
		directory.Remove(name)
		return err
	}
	return nil
}
