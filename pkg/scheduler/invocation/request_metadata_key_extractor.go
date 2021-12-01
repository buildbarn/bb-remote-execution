package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/protobuf/types/known/anypb"
)

type requestMetadataKeyExtractor struct{}

func (ke requestMetadataKeyExtractor) ExtractKey(ctx context.Context, requestMetadata *remoteexecution.RequestMetadata) (Key, error) {
	any, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: requestMetadata.GetToolInvocationId(),
	})
	if err != nil {
		return "", err
	}
	return NewKey(any)
}

// RequestMetadataKeyExtractor is an implementation of KeyExtractor that
// returns a Key that is based on the tool_invocation_id field of the
// RequestMetadata provided by a client. This will cause
// InMemoryBuildQueue to group all operations created by a single
// invocation of Bazel together, which ensures scheduling fairness.
var RequestMetadataKeyExtractor KeyExtractor = requestMetadataKeyExtractor{}
