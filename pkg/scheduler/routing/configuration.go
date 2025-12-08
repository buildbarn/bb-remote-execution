package routing

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteactionrouter"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewActionRouterFromConfiguration creates an ActionRouter based on
// options specified in a configuration file.
func NewActionRouterFromConfiguration(configuration *pb.ActionRouterConfiguration, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int, previousExecutionStatsStore initialsizeclass.PreviousExecutionStatsStore, grpcClientFactory bb_grpc.ClientFactory, dependenciesGroup program.Group) (ActionRouter, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No action router configuration provided")
	}
	switch kind := configuration.Kind.(type) {
	case *pb.ActionRouterConfiguration_Simple:
		platformKeyExtractor, err := platform.NewKeyExtractorFromConfiguration(kind.Simple.PlatformKeyExtractor, contentAddressableStorage, maximumMessageSizeBytes)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create platform key extractor")
		}
		invocationKeyExtractors := make([]invocation.KeyExtractor, 0, len(kind.Simple.InvocationKeyExtractors))
		for i, entry := range kind.Simple.InvocationKeyExtractors {
			invocationKeyExtractor, err := invocation.NewKeyExtractorFromConfiguration(entry)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to create invocation key extractor at index %d", i)
			}
			invocationKeyExtractors = append(invocationKeyExtractors, invocationKeyExtractor)
		}
		initialSizeClassAnalyzer, err := initialsizeclass.NewAnalyzerFromConfiguration(kind.Simple.InitialSizeClassAnalyzer, previousExecutionStatsStore)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create initial size class analyzer")
		}
		return NewSimpleActionRouter(platformKeyExtractor, invocationKeyExtractors, initialSizeClassAnalyzer), nil
	case *pb.ActionRouterConfiguration_Demultiplexing:
		platformKeyExtractor, err := platform.NewKeyExtractorFromConfiguration(kind.Demultiplexing.PlatformKeyExtractor, contentAddressableStorage, maximumMessageSizeBytes)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create platform key extractor")
		}
		defaultActionRouter, err := NewActionRouterFromConfiguration(kind.Demultiplexing.DefaultActionRouter, contentAddressableStorage, maximumMessageSizeBytes, previousExecutionStatsStore, grpcClientFactory, dependenciesGroup)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create default action router")
		}
		actionRouter := NewDemultiplexingActionRouter(platformKeyExtractor, defaultActionRouter)
		for _, backend := range kind.Demultiplexing.Backends {
			instanceNamePrefix, err := digest.NewInstanceName(backend.InstanceNamePrefix)
			if err != nil {
				return nil, util.StatusWrapf(err, "Invalid instance name prefix %#v", backend.InstanceNamePrefix)
			}
			backendActionRouter, err := NewActionRouterFromConfiguration(backend.ActionRouter, contentAddressableStorage, maximumMessageSizeBytes, previousExecutionStatsStore, grpcClientFactory, dependenciesGroup)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to create demultiplexing action router backend")
			}
			if err := actionRouter.RegisterActionRouter(instanceNamePrefix, backend.Platform, backendActionRouter); err != nil {
				return nil, util.StatusWrap(err, "Failed to register demultiplexing action router backend")
			}
		}
		return actionRouter, nil
	case *pb.ActionRouterConfiguration_Remote:
		client, err := grpcClientFactory.NewClientFromConfiguration(kind.Remote.GrpcClient, dependenciesGroup)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create gRPC client for remote action router")
		}
		initialSizeClassAnalyzer, err := initialsizeclass.NewAnalyzerFromConfiguration(kind.Remote.InitialSizeClassAnalyzer, previousExecutionStatsStore)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create initial size class analyzer")
		}
		return NewRemoteActionRouter(remoteactionrouter.NewActionRouterClient(client), initialSizeClassAnalyzer), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported action router type")
	}
}
