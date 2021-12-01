package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/protobuf/types/known/emptypb"
)

var emptyKey = MustNewKey(mustNewAny(&emptypb.Empty{}))

type emptyKeyExtractor struct{}

func (ke emptyKeyExtractor) ExtractKey(ctx context.Context, requestMetadata *remoteexecution.RequestMetadata) (Key, error) {
	return emptyKey, nil
}

// EmptyKeyExtractor is a simple implementation of KeyExtractor that
// returns a fixed value. This causes InMemoryBuildQueue to group all
// operations under a single invocation, meaning that no fairness
// between builds is provided.
var EmptyKeyExtractor KeyExtractor = emptyKeyExtractor{}
