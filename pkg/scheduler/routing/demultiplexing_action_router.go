package routing

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	demultiplexingActionRouterPrometheusMetrics sync.Once

	demultiplexingActionRouterRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "demultiplexing_action_router_requests_total",
			Help:      "Number of actions that were routed using the demultiplexing action router.",
		},
		[]string{"instance_name_prefix", "platform"})
)

type demultiplexingActionRouterEntry struct {
	actionRouter  ActionRouter
	requestsTotal prometheus.Counter
}

// DemultiplexingActionRouter is an implementation of ActionRouter that
// demultiplexes routing requests by REv2 instance name and platform
// properties, using a platform.Trie. This makes it possible to use
// different invocation key extractors or initial size class analyzers
// depending on the platform. When combined with
// platform.StaticKeyExtractor, it can be used to rewrite platform
// properties.
type DemultiplexingActionRouter struct {
	platformKeyExtractor platform.KeyExtractor
	trie                 *platform.Trie
	entries              []demultiplexingActionRouterEntry
}

// NewDemultiplexingActionRouter creates a new
// DemultiplexingActionRouter that forwards all incoming requests to a
// single ActionRouter.
func NewDemultiplexingActionRouter(platformKeyExtractor platform.KeyExtractor, defaultActionRouter ActionRouter) *DemultiplexingActionRouter {
	demultiplexingActionRouterPrometheusMetrics.Do(func() {
		prometheus.MustRegister(demultiplexingActionRouterRequestsTotal)
	})

	return &DemultiplexingActionRouter{
		platformKeyExtractor: platformKeyExtractor,
		trie:                 platform.NewTrie(),
		entries: []demultiplexingActionRouterEntry{
			{
				actionRouter:  defaultActionRouter,
				requestsTotal: demultiplexingActionRouterRequestsTotal.WithLabelValues("", ""),
			},
		},
	}
}

var _ ActionRouter = (*DemultiplexingActionRouter)(nil)

// RegisterActionRouter registers a new ActionRouter by REv2 instance
// name prefix and platform properties, so that subsequent calls to
// RouteAction() may forward requests to it.
func (ar *DemultiplexingActionRouter) RegisterActionRouter(instanceNamePrefix digest.InstanceName, platformMessage *remoteexecution.Platform, actionRouter ActionRouter) error {
	platformKey, err := platform.NewKey(instanceNamePrefix, platformMessage)
	if err != nil {
		return err
	}
	if ar.trie.ContainsExact(platformKey) {
		return status.Error(codes.AlreadyExists, "An action router with the same instance name prefix and platform already exists")
	}
	ar.trie.Set(platformKey, len(ar.entries)-1)
	ar.entries = append(ar.entries, demultiplexingActionRouterEntry{
		actionRouter:  actionRouter,
		requestsTotal: demultiplexingActionRouterRequestsTotal.WithLabelValues(platformKey.GetInstanceNamePrefix().String(), platformKey.GetPlatformString()),
	})
	return nil
}

// RouteAction forwards requests to one of the ActionRouters that was
// provided to NewDemultiplexingActionRouter() or
// RegisterActionRouter().
func (ar *DemultiplexingActionRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (platform.Key, invocation.Key, initialsizeclass.Selector, error) {
	key, err := ar.platformKeyExtractor.ExtractKey(ctx, digestFunction.GetInstanceName(), action)
	if err != nil {
		return platform.Key{}, "", nil, util.StatusWrap(err, "Failed to extract platform key")
	}
	entry := &ar.entries[ar.trie.GetLongestPrefix(key)+1]
	entry.requestsTotal.Inc()
	return entry.actionRouter.RouteAction(ctx, digestFunction, action, requestMetadata)
}
