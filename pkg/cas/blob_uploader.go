package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobUploader is an interface for uploading an arbitrary blob.
type BlobUploader interface {
	UploadBlob(ctx context.Context, d digest.Digest, blob Blob) error
}
