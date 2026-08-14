package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// BlobUploader is an interface for uploading an arbitrary blob.
type BlobUploader interface {
	UploadBlob(ctx context.Context, digestFunction digest.Function, blob filesystem.FileReader) (digest.Digest, error)
}
