package main

import (
	"context"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/auth"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_scheduler bb_scheduler.jsonnet")
		}
		var configuration bb_scheduler.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		browserURL, err := url.Parse(configuration.BrowserUrl)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse browser URL")
		}

		// Storage access. The scheduler requires access to the Action
		// and Command messages stored in the CAS to obtain platform
		// properties.
		info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			dependenciesGroup,
			configuration.ContentAddressableStorage,
			blobstore_configuration.NewCASBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			return util.StatusWrap(err, "Failed to create Content Adddressable Storage")
		}
		contentAddressableStorage := re_blobstore.NewExistencePreconditionBlobAccess(info.BlobAccess)

		// Optional: Initial Size Class Cache (ISCC) access. This data
		// store is only used if one or more parts of the ActionRouter
		// are configured to use feedback driven initial size class
		// analysis.
		var previousExecutionStatsStore initialsizeclass.PreviousExecutionStatsStore
		if isccConfiguration := configuration.InitialSizeClassCache; isccConfiguration != nil {
			info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
				dependenciesGroup,
				isccConfiguration,
				blobstore_configuration.NewISCCBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)))
			if err != nil {
				return util.StatusWrap(err, "Failed to create Initial Size Class Cache")
			}
			previousExecutionStatsStore = re_blobstore.NewBlobAccessMutableProtoStore[iscc.PreviousExecutionStats](
				info.BlobAccess,
				int(configuration.MaximumMessageSizeBytes))
		}

		// Create an action router that is responsible for analyzing
		// incoming execution requests and determining how they are
		// scheduled.
		actionRouter, err := routing.NewActionRouterFromConfiguration(configuration.ActionRouter, contentAddressableStorage, int(configuration.MaximumMessageSizeBytes), previousExecutionStatsStore)
		if err != nil {
			return util.StatusWrap(err, "Failed to create action router")
		}

		authorizerFactory := auth.DefaultAuthorizerFactory
		executeAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.ExecuteAuthorizer)
		if err != nil {
			return util.StatusWrap(err, "Failed to create execute authorizer")
		}

		platformQueueWithNoWorkersTimeout := configuration.PlatformQueueWithNoWorkersTimeout
		if err := platformQueueWithNoWorkersTimeout.CheckValid(); err != nil {
			return util.StatusWrap(err, "Invalid platform queue with no workers timeout")
		}

		// Create in-memory build queue.
		// TODO: Make timeouts configurable.
		generator := random.NewFastSingleThreadedGenerator()
		buildQueue := scheduler.NewInMemoryBuildQueue(
			contentAddressableStorage,
			clock.SystemClock,
			uuid.NewRandom,
			&scheduler.InMemoryBuildQueueConfiguration{
				ExecutionUpdateInterval:           time.Minute,
				OperationWithNoWaitersTimeout:     time.Minute,
				PlatformQueueWithNoWorkersTimeout: platformQueueWithNoWorkersTimeout.AsDuration(),
				BusyWorkerSynchronizationInterval: 10 * time.Second,
				GetIdleWorkerSynchronizationInterval: func() time.Duration {
					// Let synchronization calls block somewhere
					// between 0 and 2 minutes. Add jitter to
					// prevent recurring traffic spikes.
					return random.Duration(generator, 2*time.Minute)
				},
				WorkerTaskRetryCount:                9,
				WorkerWithNoSynchronizationsTimeout: time.Minute,
			},
			int(configuration.MaximumMessageSizeBytes),
			actionRouter,
			executeAuthorizer)

		// Create predeclared platform queues.
		for _, platformQueue := range configuration.PredeclaredPlatformQueues {
			instanceName, err := digest.NewInstanceName(platformQueue.InstanceNamePrefix)
			if err != nil {
				return util.StatusWrapf(err, "Invalid instance name prefix %#v", platformQueue.InstanceNamePrefix)
			}
			workerInvocationStickinessLimits := make([]time.Duration, 0, len(platformQueue.WorkerInvocationStickinessLimits))
			for i, d := range platformQueue.WorkerInvocationStickinessLimits {
				if err := d.CheckValid(); err != nil {
					return util.StatusWrapf(err, "Invalid worker invocation stickiness limit at index %d: %s", i)
				}
				workerInvocationStickinessLimits = append(workerInvocationStickinessLimits, d.AsDuration())
			}

			if err := buildQueue.RegisterPredeclaredPlatformQueue(
				instanceName,
				platformQueue.Platform,
				workerInvocationStickinessLimits,
				int(platformQueue.MaximumQueuedBackgroundLearningOperations),
				platformQueue.BackgroundLearningOperationPriority,
				platformQueue.MaximumSizeClass,
			); err != nil {
				return util.StatusWrap(err, "Failed to register predeclared platform queue")
			}
		}

		// Spawn gRPC servers for client and worker traffic.
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.ClientGrpcServers,
			func(s grpc.ServiceRegistrar) {
				remoteexecution.RegisterCapabilitiesServer(
					s,
					capabilities.NewServer(buildQueue))
				remoteexecution.RegisterExecutionServer(s, buildQueue)
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "Client gRPC server failure")
		}
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.WorkerGrpcServers,
			func(s grpc.ServiceRegistrar) {
				remoteworker.RegisterOperationQueueServer(s, buildQueue)
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "Worker gRPC server failure")
		}
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.BuildQueueStateGrpcServers,
			func(s grpc.ServiceRegistrar) {
				buildqueuestate.RegisterBuildQueueStateServer(s, buildQueue)
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "Build queue state gRPC server failure")
		}

		// Web server for metrics and profiling.
		router := mux.NewRouter()
		routePrefix := path.Join("/", configuration.AdminRoutePrefix)
		if !strings.HasSuffix(routePrefix, "/") {
			routePrefix += "/"
		}
		subrouter := router.PathPrefix(routePrefix).Subrouter()
		newBuildQueueStateService(buildQueue, clock.SystemClock, browserURL, subrouter)
		if err := http.NewServersFromConfigurationAndServe(configuration.AdminHttpServers, router, siblingsGroup); err != nil {
			return util.StatusWrap(err, "Failed to create admin HTTP servers")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
