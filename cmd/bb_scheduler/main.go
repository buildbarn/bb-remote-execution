package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
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
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_scheduler bb_scheduler.jsonnet")
	}
	var configuration bb_scheduler.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}
	terminationContext, terminationGroup := global.InstallGracefulTerminationHandler()

	browserURL, err := url.Parse(configuration.BrowserUrl)
	if err != nil {
		log.Fatal("Failed to parse browser URL: ", err)
	}

	// Storage access. The scheduler requires access to the Action
	// and Command messages stored in the CAS to obtain platform
	// properties.
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		terminationContext,
		terminationGroup,
		configuration.ContentAddressableStorage,
		blobstore_configuration.NewCASBlobAccessCreator(
			grpcClientFactory,
			int(configuration.MaximumMessageSizeBytes)))
	if err != nil {
		log.Fatal("Failed to create Content Adddressable Storage: ", err)
	}
	contentAddressableStorage := re_blobstore.NewExistencePreconditionBlobAccess(info.BlobAccess)

	// Optional: Initial Size Class Cache (ISCC) access. This data
	// store is only used if one or more parts of the ActionRouter
	// are configured to use feedback driven initial size class
	// analysis.
	var previousExecutionStatsStore initialsizeclass.PreviousExecutionStatsStore
	if isccConfiguration := configuration.InitialSizeClassCache; isccConfiguration != nil {
		info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			terminationContext,
			terminationGroup,
			isccConfiguration,
			blobstore_configuration.NewISCCBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Initial Size Class Cache: ", err)
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
		log.Fatal("Failed to create action router: ", err)
	}

	authorizerFactory := auth.DefaultAuthorizerFactory
	executeAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.ExecuteAuthorizer)
	if err != nil {
		log.Fatal("Failed to create execute authorizer: ", err)
	}

	platformQueueWithNoWorkersTimeout := configuration.PlatformQueueWithNoWorkersTimeout
	if err := platformQueueWithNoWorkersTimeout.CheckValid(); err != nil {
		log.Fatal("Invalid platform queue with no workers timeout: ", err)
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
			log.Fatalf("Invalid instance name prefix %#v: %s", platformQueue.InstanceNamePrefix, err)
		}
		workerInvocationStickinessLimits := make([]time.Duration, 0, len(platformQueue.WorkerInvocationStickinessLimits))
		for i, d := range platformQueue.WorkerInvocationStickinessLimits {
			if err := d.CheckValid(); err != nil {
				log.Fatalf("Invalid worker invocation stickiness limit at index %d: %s", i, err)
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
			log.Fatal("Failed to register predeclared platform queue: ", err)
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
		}); err != nil {
		log.Fatal("Client gRPC server failure: ", err)
	}
	if err := bb_grpc.NewServersFromConfigurationAndServe(
		configuration.WorkerGrpcServers,
		func(s grpc.ServiceRegistrar) {
			remoteworker.RegisterOperationQueueServer(s, buildQueue)
		}); err != nil {
		log.Fatal("Worker gRPC server failure: ", err)
	}
	if err := bb_grpc.NewServersFromConfigurationAndServe(
		configuration.BuildQueueStateGrpcServers,
		func(s grpc.ServiceRegistrar) {
			buildqueuestate.RegisterBuildQueueStateServer(s, buildQueue)
		}); err != nil {
		log.Fatal("Build queue state gRPC server failure: ", err)
	}

	// Automatically drain workers based on AWS ASG lifecycle events.
	if len(configuration.AwsAsgLifecycleHooks) > 0 {
		cfg, err := aws.NewConfigFromConfiguration(configuration.AwsSession, "LifecycleHookSQSMessageHandler")
		if err != nil {
			log.Fatal("Failed to create AWS session: ", err)
		}
		autoScalingClient := autoscaling.NewFromConfig(cfg)
		sqsClient := sqs.NewFromConfig(cfg)
		for _, lifecycleHook := range configuration.AwsAsgLifecycleHooks {
			r := re_aws.NewSQSReceiver(
				sqsClient,
				lifecycleHook.SqsUrl,
				10*time.Minute,
				re_aws.NewLifecycleHookSQSMessageHandler(
					autoScalingClient,
					re_aws.NewBuildQueueLifecycleHookHandler(
						buildQueue,
						lifecycleHook.InstanceIdLabel)),
				util.DefaultErrorLogger)
			go func() {
				for {
					if err := r.PerformSingleRequest(); err != nil {
						log.Print("Failed to receive messages from SQS: ", err)
						time.Sleep(10 * time.Second)
					}
				}
			}()
		}
	}

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	newBuildQueueStateService(buildQueue, clock.SystemClock, browserURL, router)
	go func() {
		log.Fatal(http.ListenAndServe(configuration.AdminHttpListenAddress, router))
	}()

	lifecycleState.MarkReadyAndWait()
}
