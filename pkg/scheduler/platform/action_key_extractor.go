package platform

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type actionKeyExtractor struct{}

func (ke actionKeyExtractor) ExtractKey(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (Key, error) {
	return NewKey(digestFunction.GetInstanceName(), action.Platform)
}

// ActionKeyExtractor is capable of extracting a platform key from an
// REv2 Action message.
//
// Because it does not fall back to reading platform properties from the
// Command message, it is only capable of processing requests sent by a
// client that implements REv2.2 or newer.
var ActionKeyExtractor KeyExtractor = actionKeyExtractor{}
