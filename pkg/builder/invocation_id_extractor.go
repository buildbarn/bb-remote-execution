package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/types/known/anypb"
)

// InvocationIDExtractor is a helper type that is used by
// InMemoryBuildQueue to extract an invocation ID message out of
// incoming requests. The invocation ID is an identifier for a set of
// operations that should be scheduled collectively and fairly with
// respect to other sets.
type InvocationIDExtractor interface {
	ExtractInvocationID(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*anypb.Any, error)
}

type requestMetadataInvocationIDExtractor struct{}

func (ide requestMetadataInvocationIDExtractor) ExtractInvocationID(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*anypb.Any, error) {
	return anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: requestMetadata.GetToolInvocationId(),
	})
}

// RequestMetadataInvocationIDExtractor is a default implementation of
// InvocationIDExtractor that creates invocation ID messages that simply
// contain the 'tool_invocation_id' value provided by REv2 clients. When
// used, separate client invocations will all be scheduled fairly with
// respect to each other.
var RequestMetadataInvocationIDExtractor InvocationIDExtractor = requestMetadataInvocationIDExtractor{}
