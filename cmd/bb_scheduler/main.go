package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/sqs"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/builder/initialsizeclass"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
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
	lifecycleState, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	browserURL, err := url.Parse(configuration.BrowserUrl)
	if err != nil {
		log.Fatal("Failed to parse browser URL: ", err)
	}

	// Storage access. The scheduler requires access to the Action
	// and Command messages stored in the CAS to obtain platform
	// properties.
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		configuration.ContentAddressableStorage,
		blobstore_configuration.NewCASBlobAccessCreator(
			bb_grpc.DefaultClientFactory,
			int(configuration.MaximumMessageSizeBytes)))
	if err != nil {
		log.Fatal("Failed to create Content Adddressable Storage: ", err)
	}
	contentAddressableStorage := re_blobstore.NewExistencePreconditionBlobAccess(info.BlobAccess)

	// Platform queues that are not predeclared may not use multiple
	// size classes. For those platform queues it is sufficient to
	// just use a naive initial size class analyzer.
	defaultExecutionTimeout := configuration.DefaultExecutionTimeout
	if err := defaultExecutionTimeout.CheckValid(); err != nil {
		log.Fatal("Invalid default execution timeout: ", err)
	}
	maximumExecutionTimeout := configuration.MaximumExecutionTimeout
	if err := maximumExecutionTimeout.CheckValid(); err != nil {
		log.Fatal("Invalid maximum execution timeout: ", err)
	}
	actionTimeoutExtractor := initialsizeclass.NewActionTimeoutExtractor(
		defaultExecutionTimeout.AsDuration(),
		maximumExecutionTimeout.AsDuration())
	defaultInitialSizeClassAnalyzer := initialsizeclass.NewFallbackAnalyzer(actionTimeoutExtractor)

	// Create in-memory build queue.
	// TODO: Make timeouts configurable.
	generator := random.NewFastSingleThreadedGenerator()
	buildQueue := builder.NewInMemoryBuildQueue(
		contentAddressableStorage,
		clock.SystemClock,
		uuid.NewRandom,
		&builder.InMemoryBuildQueueConfiguration{
			ExecutionUpdateInterval:           time.Minute,
			OperationWithNoWaitersTimeout:     time.Minute,
			PlatformQueueWithNoWorkersTimeout: 15 * time.Minute,
			BusyWorkerSynchronizationInterval: 10 * time.Second,
			GetIdleWorkerSynchronizationInterval: func() time.Duration {
				// Let synchronization calls block somewhere
				// between 1 and 2 minutes. Add jitter to
				// prevent recurring traffic spikes.
				return time.Minute + time.Duration(generator.Intn(60*1e6))*time.Microsecond
			},
			WorkerTaskRetryCount:                9,
			WorkerWithNoSynchronizationsTimeout: time.Minute,
		},
		int(configuration.MaximumMessageSizeBytes),
		struct {
			builder.InvocationIDExtractor
			initialsizeclass.Analyzer
		}{
			InvocationIDExtractor: builder.RequestMetadataInvocationIDExtractor,
			Analyzer:              defaultInitialSizeClassAnalyzer,
		})

	// Create predeclared platform queues.
	for _, platformQueue := range configuration.PredeclaredPlatformQueues {
		instanceName, err := digest.NewInstanceName(platformQueue.InstanceNamePrefix)
		if err != nil {
			log.Fatalf("Invalid instance name prefix %#v: %s", platformQueue.InstanceNamePrefix, err)
		}
		workerInvocationStickinessLimit := platformQueue.WorkerInvocationStickinessLimit
		if err := workerInvocationStickinessLimit.CheckValid(); err != nil {
			log.Fatal("Invalid worker invocation stickiness limit: ", err)
		}

		invocationIDExtractor := builder.RequestMetadataInvocationIDExtractor

		// Create an analyzer for picking an initial size class.
		// This lets bb_scheduler cache execution times and
		// outcomes of actions, so that it can more accurately
		// pick size classes in the future.
		initialSizeClassAnalyzer := defaultInitialSizeClassAnalyzer
		var maximumQueuedBackgroundLearningOperations int
		var backgroundLearningOperationPriority int32
		if fdaConfiguration := platformQueue.InitialSizeClassFeedbackDrivenAnalyzer; fdaConfiguration != nil {
			info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
				fdaConfiguration.InitialSizeClassCache,
				blobstore_configuration.NewISCCBlobAccessCreator(
					bb_grpc.DefaultClientFactory,
					int(configuration.MaximumMessageSizeBytes)))
			if err != nil {
				log.Fatal("Failed to create Initial Size Class Cache: ", err)
			}

			failureCacheDuration := fdaConfiguration.FailureCacheDuration
			if err := failureCacheDuration.CheckValid(); err != nil {
				log.Fatal("Invalid failure cache duration: ", err)
			}

			minimumExecutionTimeout := fdaConfiguration.MinimumExecutionTimeout
			if err := minimumExecutionTimeout.CheckValid(); err != nil {
				log.Fatal("Invalid minimum acceptable execution time: ", err)
			}

			initialSizeClassAnalyzer = initialsizeclass.NewFeedbackDrivenAnalyzer(
				initialsizeclass.NewBlobAccessPreviousExecutionStatsStore(
					info.BlobAccess,
					int(configuration.MaximumMessageSizeBytes)),
				random.NewFastSingleThreadedGenerator(),
				clock.SystemClock,
				actionTimeoutExtractor,
				failureCacheDuration.AsDuration(),
				initialsizeclass.NewPageRankStrategyCalculator(
					minimumExecutionTimeout.AsDuration(),
					fdaConfiguration.AcceptableExecutionTimeIncreaseExponent,
					fdaConfiguration.SmallerSizeClassExecutionTimeoutMultiplier,
					fdaConfiguration.MaximumConvergenceError),
				int(fdaConfiguration.HistorySize))
			maximumQueuedBackgroundLearningOperations = int(fdaConfiguration.MaximumQueuedBackgroundLearningOperations)
			backgroundLearningOperationPriority = fdaConfiguration.BackgroundLearningOperationPriority
		}

		if err := buildQueue.RegisterPredeclaredPlatformQueue(
			instanceName,
			platformQueue.Platform,
			workerInvocationStickinessLimit.AsDuration(),
			maximumQueuedBackgroundLearningOperations,
			backgroundLearningOperationPriority,
			platformQueue.MaximumSizeClass,
			struct {
				builder.InvocationIDExtractor
				initialsizeclass.Analyzer
			}{
				InvocationIDExtractor: invocationIDExtractor,
				Analyzer:              initialSizeClassAnalyzer,
			}); err != nil {
			log.Fatal("Failed to register predeclared platform queue: ", err)
		}
	}

	// Spawn gRPC servers for client and worker traffic.
	go func() {
		log.Fatal(
			"Client gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.ClientGrpcServers,
				func(s *grpc.Server) {
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()
	go func() {
		log.Fatal(
			"Worker gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.WorkerGrpcServers,
				func(s *grpc.Server) {
					remoteworker.RegisterOperationQueueServer(s, buildQueue)
				}))
	}()
	go func() {
		log.Fatal(
			"Build queue state gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.BuildQueueStateGrpcServers,
				func(s *grpc.Server) {
					buildqueuestate.RegisterBuildQueueStateServer(s, buildQueue)
				}))
	}()

	// Automatically drain workers based on AWS ASG lifecycle events.
	if len(configuration.AwsAsgLifecycleHooks) > 0 {
		sess, err := aws.NewSessionFromConfiguration(configuration.AwsSession)
		if err != nil {
			log.Fatal("Failed to create AWS session: ", err)
		}
		autoScaling := autoscaling.New(sess)
		sqs := sqs.New(sess)
		for _, lifecycleHook := range configuration.AwsAsgLifecycleHooks {
			r := re_aws.NewSQSReceiver(
				sqs,
				lifecycleHook.SqsUrl,
				10*time.Minute,
				re_aws.NewLifecycleHookSQSMessageHandler(
					autoScaling,
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
