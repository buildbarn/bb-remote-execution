package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

func PutBlob(ctx context.Context, contentAddressableStorage cas.ContentAddressableStorage, d digest.Digest, blob Blob) error {
	params, err := contentAddressableStorage.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		blob.Discard()
		return util.StatusWrap(err, "Could not fetch CDC parameters")
	}

	// For small blobs, extracting the full byte slice is most optimal and hooks natively
	// into PutChunk without the overhead of initializing the chunker stream inside PutReader.
	if cas.IsSingleChunk(params, d) {
		data, err := blob.ToByteSlice()
		if err != nil {
			return err
		}
		return contentAddressableStorage.PutChunk(ctx, d, data)
	}

	// For larger blobs, we rely on the single-threaded chunker implementation in PutReader
	// to process the stream without loading it completely into memory.
	r := blob.ToReaderAt()
	defer r.Close()
	return cas.PutReader(ctx, contentAddressableStorage, d, io.NewSectionReader(r, 0, d.GetSizeBytes()))
}
