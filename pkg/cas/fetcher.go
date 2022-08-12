package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Fetcher is responsible for fetching blobs from the Content
// Addressable Storage (CAS) that can be parsed and represented as a
// native data type. Examples are Protobuf messages such as REv2 Action,
// Command, Directory and Tree objects.
type Fetcher[T any] interface {
	Get(ctx context.Context, digest digest.Digest) (*T, error)
}
