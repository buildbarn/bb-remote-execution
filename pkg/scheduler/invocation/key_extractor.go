package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// KeyExtractor is responsible for extracting an invocation key from an
// incoming execution request. Operations will be grouped by invocation
// key and scheduled fairly.
//
// Implementations of KeyExtract may construct keys based on REv2
// request metadata or user credentials.
type KeyExtractor interface {
	ExtractKey(ctx context.Context, requestMetadata *remoteexecution.RequestMetadata) (Key, error)
}
