package routing

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// ActionRouter is responsible for doing all forms of analysis on an
// incoming execution request up to the point where InMemoryBuildQueue
// acquires locks and enqueues an operation. ActionRouter is responsible
// for the following things:
//
// - To extract a platform key from the action, so that
//   InMemoryBuildQueue knows on which workers the action needs to
//   execute.
// - To extract an invocation key from the client's context, so that
//   InMemoryBuildQueue can group operations belonging to the same
//   client and schedule them fairly with respect to other clients.
// - To create an initial size class selector, which InMemoryBuildQueue
//   can use to select the appropriate worker size.
type ActionRouter interface {
	RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (platform.Key, invocation.Key, initialsizeclass.Selector, error)
}
