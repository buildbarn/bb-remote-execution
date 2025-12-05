package routing

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteactionrouter"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type remoteActionRouter struct {
	client                   remoteactionrouter.ActionRouterClient
	initialSizeClassAnalyzer initialsizeclass.Analyzer
}

// NewRemoteActionRouter creates an ActionRouter that delegates routing
// decisions to a remote gRPC service.  Initial size class selection is
// performed locally using the provided analyzer.
func NewRemoteActionRouter(client remoteactionrouter.ActionRouterClient, initialSizeClassAnalyzer initialsizeclass.Analyzer) ActionRouter {
	return &remoteActionRouter{
		client:                   client,
		initialSizeClassAnalyzer: initialSizeClassAnalyzer,
	}
}

func (ar *remoteActionRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*remoteexecution.Action, platform.Key, []invocation.Key, initialsizeclass.Selector, error) {
	response, err := ar.client.RouteAction(ctx, &remoteactionrouter.RouteActionRequest{
		InstanceName:    digestFunction.GetInstanceName().String(),
		DigestFunction:  digestFunction.GetEnumValue(),
		Action:          action,
		RequestMetadata: requestMetadata,
	})
	if err != nil {
		return nil, platform.Key{}, nil, nil, util.StatusWrap(err, "Failed to route action via remote service")
	}

	action = response.Action
	if action == nil {
		return nil, platform.Key{}, nil, nil, status.Error(codes.Internal, "Remote action router response does not contain an action")
	}
	if action.Platform == nil {
		return nil, platform.Key{}, nil, nil, status.Error(codes.Internal, "Remote action router response action does not contain a platform")
	}

	platformKey, err := platform.NewKey(digestFunction.GetInstanceName(), action.Platform)
	if err != nil {
		return nil, platform.Key{}, nil, nil, util.StatusWrapWithCode(err, codes.Internal, "Malformed platform in remote action router response")
	}

	if len(response.InvocationKeys) == 0 {
		return nil, platform.Key{}, nil, nil, status.Error(codes.Internal, "Invalid remote action router response does not contain any invocation keys")
	}
	invocationKeys := make([]invocation.Key, 0, len(response.InvocationKeys))
	for _, anyKey := range response.InvocationKeys {
		invocationKey, err := invocation.NewKey(anyKey)
		if err != nil {
			return nil, platform.Key{}, nil, nil, util.StatusWrapWithCode(err, codes.Internal, "Malformed invocation key in remote action router response")
		}
		invocationKeys = append(invocationKeys, invocationKey)
	}

	// Initial size class analysis is done locally.
	initialSizeClassSelector, err := ar.initialSizeClassAnalyzer.Analyze(ctx, digestFunction, action)
	if err != nil {
		return nil, platform.Key{}, nil, nil, util.StatusWrap(err, "Initial size class analysis failed")
	}

	return action, platformKey, invocationKeys, initialSizeClassSelector, nil
}
