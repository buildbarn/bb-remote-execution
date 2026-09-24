package cas

import (
	"bytes"
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	bb_cas "github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/protobuf/proto"
)

// PutProto is a helper function for storing Protobuf messages in the
// Content Addressable Storage (CAS). It computes the digest of the
// message and stores it under that key. The digest is then returned, so
// that the object may be referenced.
func PutProto(ctx context.Context, zstdPool zstd.Pool, chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkListStorage blobstore.BlobAccess[chunk.List], params *remoteexecution.RepMaxCdcParams, message proto.Message, digestFunction digest.Function) (digest.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}
	digestGenerator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := digestGenerator.Write(data); err != nil {
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()
	if err := bb_cas.PutReader(ctx, zstdPool, chunkStorage, chunkListStorage, params, blobDigest, bytes.NewReader(data)); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
