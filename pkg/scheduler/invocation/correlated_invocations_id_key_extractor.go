package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/protobuf/types/known/anypb"
)

type correlatedInvocationsIDKeyExtractor struct{}

func (ke correlatedInvocationsIDKeyExtractor) ExtractKey(ctx context.Context, requestMetadata *remoteexecution.RequestMetadata) (Key, error) {
	any, err := anypb.New(&remoteexecution.RequestMetadata{
		CorrelatedInvocationsId: requestMetadata.GetCorrelatedInvocationsId(),
	})
	if err != nil {
		return "", err
	}
	return NewKey(any)
}

// CorrelatedInvocationsIDKeyExtractor is an implementation of
// KeyExtractor that returns a Key that is based on the
// correlated_invocations_id field of the RequestMetadata provided by a
// client. This will cause InMemoryBuildQueue to group all operations
// created by all invocation of Bazel that use the same
// --build_request_id together, which ensures scheduling fairness.
var CorrelatedInvocationsIDKeyExtractor KeyExtractor = correlatedInvocationsIDKeyExtractor{}
