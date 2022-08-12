package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessDirectoryFetcher struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessDirectoryFetcher creates a DirectoryFetcher that reads
// Directory objects from a BlobAccess based store.
func NewBlobAccessDirectoryFetcher(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) DirectoryFetcher {
	return &blobAccessDirectoryFetcher{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (df *blobAccessDirectoryFetcher) Get(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.blobAccess.Get(ctx, digest).ToProto(&remoteexecution.Directory{}, df.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}
