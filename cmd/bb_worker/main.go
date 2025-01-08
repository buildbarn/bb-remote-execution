package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	re_clock "github.com/buildbarn/bb-remote-execution/pkg/clock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_worker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/otel"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_worker bb_worker.jsonnet")
		}
		var configuration bb_worker.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}
		tracerProvider := otel.GetTracerProvider()

		browserURL, err := url.Parse(configuration.BrowserUrl)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse browser URL")
		}

		// Create connection with scheduler.
		schedulerConnection, err := grpcClientFactory.NewClientFromConfiguration(configuration.Scheduler)
		if err != nil {
			return util.StatusWrap(err, "Failed to create scheduler RPC client")
		}
		schedulerClient := remoteworker.NewOperationQueueClient(schedulerConnection)

		// Location for storing temporary file objects. This is
		// currently only used by the virtual file system to store
		// output files of build actions. Going forward, this may be
		// used to store core dumps generated by build actions as well.
		filePool, err := re_filesystem.NewFilePoolFromConfiguration(configuration.FilePool)
		if err != nil {
			return util.StatusWrap(err, "Failed to create file pool")
		}

		// Storage access.
		globalContentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
			dependenciesGroup,
			configuration.Blobstore,
			grpcClientFactory,
			int(configuration.MaximumMessageSizeBytes))
		if err != nil {
			return err
		}
		globalContentAddressableStorage = re_blobstore.NewExistencePreconditionBlobAccess(globalContentAddressableStorage)

		var fileSystemAccessCache blobstore.BlobAccess
		prefetchingConfiguration := configuration.Prefetching
		if prefetchingConfiguration != nil {
			info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
				dependenciesGroup,
				prefetchingConfiguration.FileSystemAccessCache,
				blobstore_configuration.NewFSACBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)))
			if err != nil {
				return util.StatusWrap(err, "Failed to create File System Access Cache")
			}
			fileSystemAccessCache = info.BlobAccess
		}

		// Cached read access for Directory objects stored in the
		// Content Addressable Storage. All workers make use of the same
		// cache, to increase the hit rate. This process does not read
		// Tree objects.
		directoryFetcher, err := cas.NewCachingDirectoryFetcherFromConfiguration(
			configuration.DirectoryCache,
			cas.NewBlobAccessDirectoryFetcher(
				globalContentAddressableStorage,
				/* maximumDirectorySizeBytes = */ int(configuration.MaximumMessageSizeBytes),
				/* maximumTreeSizeBytes = */ 0))
		if err != nil {
			return util.StatusWrap(err, "Failed to create caching directory fetcher")
		}

		if len(configuration.BuildDirectories) == 0 {
			return status.Error(codes.InvalidArgument, "Cannot start worker without any build directories")
		}

		// Setup the RemoteCompletedActionLogger for the
		// ActionLoggingBuildExecutor to ensure we only create
		// one client per worker rather than one per runner.
		type remoteCompletedActionLogger struct {
			logger              builder.CompletedActionLogger
			instanceNamePatcher digest.InstanceNamePatcher
		}
		remoteCompletedActionLoggers := make([]remoteCompletedActionLogger, 0, len(configuration.CompletedActionLoggers))
		for _, c := range configuration.CompletedActionLoggers {
			loggerQueueConnection, err := grpcClientFactory.NewClientFromConfiguration(c.Client)
			if err != nil {
				return util.StatusWrap(err, "Failed to create a new gRPC client for logging completed actions")
			}
			client := cal_proto.NewCompletedActionLoggerClient(loggerQueueConnection)
			logger := builder.NewRemoteCompletedActionLogger(int(c.MaximumSendQueueSize), client)
			instanceNamePrefix, err := digest.NewInstanceName(c.AddInstanceNamePrefix)
			if err != nil {
				return util.StatusWrapf(err, "Invalid instance name prefix %#v", c.AddInstanceNamePrefix)
			}
			remoteCompletedActionLoggers = append(remoteCompletedActionLoggers, remoteCompletedActionLogger{
				logger:              logger,
				instanceNamePatcher: digest.NewInstanceNamePatcher(digest.EmptyInstanceName, instanceNamePrefix),
			})
			// TODO: Run this as part of the program.Group,
			// so that it gets cleaned up upon shutdown.
			go func() {
				generator := random.NewFastSingleThreadedGenerator()
				for {
					log.Print("Failure encountered while transmitting completed actions: ", logger.SendAllCompletedActions())
					time.Sleep(random.Duration(generator, 5*time.Second))
				}
			}()
		}

		inputDownloadConcurrency := configuration.InputDownloadConcurrency
		if inputDownloadConcurrency <= 0 {
			return status.Errorf(codes.InvalidArgument, "Nonpositive input download concurrency: ", inputDownloadConcurrency)
		}
		inputDownloadConcurrencySemaphore := semaphore.NewWeighted(inputDownloadConcurrency)

		outputUploadConcurrency := configuration.OutputUploadConcurrency
		if outputUploadConcurrency <= 0 {
			return status.Errorf(codes.InvalidArgument, "Nonpositive output upload concurrency: ", outputUploadConcurrency)
		}
		outputUploadConcurrencySemaphore := semaphore.NewWeighted(outputUploadConcurrency)

		testInfrastructureFailureShutdownState := builder.NewTestInfrastructureFailureShutdownState()
		for _, buildDirectoryConfiguration := range configuration.BuildDirectories {
			var virtualBuildDirectory virtual.PrepopulatedDirectory
			var handleAllocator virtual.StatefulHandleAllocator
			var symlinkFactory virtual.SymlinkFactory
			var characterDeviceFactory virtual.CharacterDeviceFactory
			var naiveBuildDirectory filesystem.DirectoryCloser
			var fileFetcher cas.FileFetcher
			var buildDirectoryCleaner cleaner.Cleaner
			uploadBatchSize := blobstore.RecommendedFindMissingDigestsCount
			var maximumExecutionTimeoutCompensation time.Duration
			var maximumWritableFileUploadDelay time.Duration
			switch backend := buildDirectoryConfiguration.Backend.(type) {
			case *bb_worker.BuildDirectoryConfiguration_Virtual:
				var mount virtual_configuration.Mount
				mount, handleAllocator, err = virtual_configuration.NewMountFromConfiguration(
					backend.Virtual.Mount,
					"bb_worker",
					/* rootDirectory = */ virtual_configuration.ShortAttributeCaching,
					/* childDirectories = */ virtual_configuration.LongAttributeCaching,
					/* leaves = */ virtual_configuration.LongAttributeCaching)
				if err != nil {
					return util.StatusWrap(err, "Failed to create build directory mount")
				}

				hiddenFilesPattern := func(s string) bool { return false }
				if pattern := backend.Virtual.HiddenFilesPattern; pattern != "" {
					hiddenFilesRegexp, err := regexp.Compile(pattern)
					if err != nil {
						return util.StatusWrap(err, "Failed to parse hidden files pattern")
					}
					hiddenFilesPattern = hiddenFilesRegexp.MatchString
				}

				initialContentsSorter := sort.Sort
				if backend.Virtual.ShuffleDirectoryListings {
					initialContentsSorter = virtual.Shuffle
				}
				symlinkFactory = virtual.NewHandleAllocatingSymlinkFactory(
					virtual.BaseSymlinkFactory,
					handleAllocator.New())
				characterDeviceFactory = virtual.NewHandleAllocatingCharacterDeviceFactory(
					virtual.BaseCharacterDeviceFactory,
					handleAllocator.New())
				virtualBuildDirectory = virtual.NewInMemoryPrepopulatedDirectory(
					virtual.NewHandleAllocatingFileAllocator(
						virtual.NewPoolBackedFileAllocator(
							re_filesystem.EmptyFilePool,
							util.DefaultErrorLogger),
						handleAllocator),
					symlinkFactory,
					util.DefaultErrorLogger,
					handleAllocator,
					initialContentsSorter,
					hiddenFilesPattern,
					clock.SystemClock)

				if err := mount.Expose(dependenciesGroup, virtualBuildDirectory); err != nil {
					return util.StatusWrap(err, "Failed to expose build directory mount")
				}

				buildDirectoryCleaner = func(ctx context.Context) error {
					if err := virtualBuildDirectory.RemoveAllChildren(false); err != nil {
						return util.StatusWrapWithCode(err, codes.Internal, "Failed to clean virtual build directory")
					}
					return nil
				}
				if err := backend.Virtual.MaximumExecutionTimeoutCompensation.CheckValid(); err != nil {
					return util.StatusWrap(err, "Invalid maximum execution timeout compensation")
				}
				maximumExecutionTimeoutCompensation = backend.Virtual.MaximumExecutionTimeoutCompensation.AsDuration()
				if err := backend.Virtual.MaximumWritableFileUploadDelay.CheckValid(); err != nil {
					return util.StatusWrap(err, "Invalid maximum writable file upload delay")
				}
				maximumWritableFileUploadDelay = backend.Virtual.MaximumWritableFileUploadDelay.AsDuration()
			case *bb_worker.BuildDirectoryConfiguration_Native:
				// Directory where actual builds take place.
				nativeConfiguration := backend.Native
				naiveBuildDirectory, err = filesystem.NewLocalDirectory(path.LocalFormat.NewParser(nativeConfiguration.BuildDirectoryPath))
				if err != nil {
					return util.StatusWrapf(err, "Failed to open build directory %v", nativeConfiguration.BuildDirectoryPath)
				}
				buildDirectoryCleaner = cleaner.NewDirectoryCleaner(naiveBuildDirectory, nativeConfiguration.BuildDirectoryPath)

				// Create a cache directory that holds input
				// files that can be hardlinked into build
				// directory.
				//
				// TODO: Have a single process-wide hardlinking
				// cache even if multiple build directories are
				// used. This increases cache hit rate.
				cacheDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(nativeConfiguration.CacheDirectoryPath))
				if err != nil {
					return util.StatusWrapf(err, "Failed to open cache directory %#v", nativeConfiguration.CacheDirectoryPath)
				}
				if err := cacheDirectory.RemoveAllChildren(); err != nil {
					return util.StatusWrapf(err, "Failed to clear cache directory %#v", nativeConfiguration.CacheDirectoryPath)
				}
				evictionSet, err := eviction.NewSetFromConfiguration[string](nativeConfiguration.CacheReplacementPolicy)
				if err != nil {
					return util.StatusWrap(err, "Failed to create eviction set for cache directory")
				}
				fileFetcher = cas.NewHardlinkingFileFetcher(
					cas.NewBlobAccessFileFetcher(globalContentAddressableStorage),
					cacheDirectory,
					int(nativeConfiguration.MaximumCacheFileCount),
					nativeConfiguration.MaximumCacheSizeBytes,
					eviction.NewMetricsSet(evictionSet, "HardlinkingFileFetcher"))

				// Using a native file system requires us to
				// hold on to file descriptors while uploading
				// outputs. Limit the batch size to ensure that
				// we don't exhaust file descriptors.
				uploadBatchSize = 100
			default:
				return status.Error(codes.InvalidArgument, "No build directory specified")
			}

			buildDirectoryIdleInvoker := cleaner.NewIdleInvoker(buildDirectoryCleaner)
			var sharedBuildDirectoryNextParallelActionID atomic.Uint64
			if len(buildDirectoryConfiguration.Runners) == 0 {
				return util.StatusWrap(err, "Cannot start worker without any runners")
			}
			for _, runnerConfiguration := range buildDirectoryConfiguration.Runners {
				if runnerConfiguration.Concurrency < 1 {
					return status.Error(codes.InvalidArgument, "Runner concurrency must be positive")
				}
				concurrencyLength := len(strconv.FormatUint(runnerConfiguration.Concurrency-1, 10))

				// Obtain raw device numbers of character
				// devices that need to be available within the
				// input root.
				inputRootCharacterDevices, err := getInputRootCharacterDevices(
					runnerConfiguration.InputRootCharacterDeviceNodes)
				if err != nil {
					return err
				}

				// Execute commands using a separate runner process. Due to the
				// interaction between threads, forking and execve() returning
				// ETXTBSY, concurrent execution of build actions can only be
				// used in combination with a runner process. Having a separate
				// runner process also makes it possible to apply privilege
				// separation.
				runnerConnection, err := grpcClientFactory.NewClientFromConfiguration(runnerConfiguration.Endpoint)
				if err != nil {
					return util.StatusWrap(err, "Failed to create runner RPC client")
				}
				runnerClient := runner_pb.NewRunnerClient(runnerConnection)

				for threadID := uint64(0); threadID < runnerConfiguration.Concurrency; threadID++ {
					// Per-worker separate writer of the Content
					// Addressable Storage that batches writes after
					// completing the build action.
					contentAddressableStorageWriter, contentAddressableStorageFlusher := re_blobstore.NewBatchedStoreBlobAccess(
						globalContentAddressableStorage,
						digest.KeyWithoutInstance,
						uploadBatchSize,
						outputUploadConcurrencySemaphore)
					contentAddressableStorageWriter = blobstore.NewMetricsBlobAccess(
						contentAddressableStorageWriter,
						clock.SystemClock,
						"cas",
						"batched_store")

					// When the virtual file system is
					// enabled, we can lazily load the input
					// root, as opposed to explicitly
					// instantiating it before every build.
					var executionTimeoutClock clock.Clock
					var buildDirectory builder.BuildDirectory
					if virtualBuildDirectory != nil {
						suspendableClock := re_clock.NewSuspendableClock(
							clock.SystemClock,
							maximumExecutionTimeoutCompensation,
							time.Second/10)
						executionTimeoutClock = suspendableClock
						buildDirectory = builder.NewVirtualBuildDirectory(
							virtualBuildDirectory,
							cas.NewSuspendingDirectoryFetcher(
								directoryFetcher,
								suspendableClock),
							re_blobstore.NewSuspendingBlobAccess(
								contentAddressableStorageWriter,
								suspendableClock),
							symlinkFactory,
							characterDeviceFactory,
							handleAllocator)
					} else {
						executionTimeoutClock = clock.SystemClock
						buildDirectory = builder.NewNaiveBuildDirectory(
							naiveBuildDirectory,
							directoryFetcher,
							fileFetcher,
							inputDownloadConcurrencySemaphore,
							contentAddressableStorageWriter)
					}

					// Create a per-action subdirectory in
					// the build directory named after the
					// action digest, so that multiple
					// actions may be run concurrently.
					//
					// Also clean the build directory every
					// time when going from fully idle to
					// executing one action.
					buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(
						builder.NewCleanBuildDirectoryCreator(
							builder.NewRootBuildDirectoryCreator(buildDirectory),
							buildDirectoryIdleInvoker),
						&sharedBuildDirectoryNextParallelActionID)

					workerID := map[string]string{}
					if runnerConfiguration.Concurrency > 1 {
						workerID["thread"] = fmt.Sprintf("%0*d", concurrencyLength, threadID)
					}
					for k, v := range runnerConfiguration.WorkerId {
						workerID[k] = v
					}
					workerName, err := json.Marshal(workerID)
					if err != nil {
						return util.StatusWrap(err, "Failed to marshal worker ID")
					}

					buildExecutor := builder.NewLocalBuildExecutor(
						contentAddressableStorageWriter,
						buildDirectoryCreator,
						runnerClient,
						executionTimeoutClock,
						maximumWritableFileUploadDelay,
						inputRootCharacterDevices,
						int(configuration.MaximumMessageSizeBytes),
						runnerConfiguration.EnvironmentVariables,
						configuration.ForceUploadTreesAndDirectories)

					if prefetchingConfiguration != nil {
						buildExecutor = builder.NewPrefetchingBuildExecutor(
							buildExecutor,
							globalContentAddressableStorage,
							directoryFetcher,
							inputDownloadConcurrencySemaphore,
							fileSystemAccessCache,
							int(configuration.MaximumMessageSizeBytes),
							int(prefetchingConfiguration.BloomFilterBitsPerPath),
							int(prefetchingConfiguration.BloomFilterMaximumSizeBytes))
					}

					buildExecutor = builder.NewMetricsBuildExecutor(
						builder.NewFilePoolStatsBuildExecutor(
							builder.NewTimestampedBuildExecutor(
								builder.NewStorageFlushingBuildExecutor(
									buildExecutor,
									contentAddressableStorageFlusher),
								clock.SystemClock,
								string(workerName))))

					if len(runnerConfiguration.CostsPerSecond) > 0 {
						buildExecutor = builder.NewCostComputingBuildExecutor(buildExecutor, runnerConfiguration.CostsPerSecond)
					}

					if maximumConsecutiveFailures := runnerConfiguration.MaximumConsecutiveTestInfrastructureFailures; maximumConsecutiveFailures > 0 {
						buildExecutor = builder.NewTestInfrastructureFailureDetectingBuildExecutor(
							buildExecutor,
							testInfrastructureFailureShutdownState,
							maximumConsecutiveFailures)
					}

					buildExecutor = builder.NewCachingBuildExecutor(
						buildExecutor,
						globalContentAddressableStorage,
						actionCache,
						browserURL)

					for _, remoteCompletedActionLogger := range remoteCompletedActionLoggers {
						buildExecutor = builder.NewCompletedActionLoggingBuildExecutor(
							buildExecutor,
							uuid.NewRandom,
							remoteCompletedActionLogger.logger,
							remoteCompletedActionLogger.instanceNamePatcher)
					}

					buildExecutor = builder.NewTracingBuildExecutor(
						builder.NewLoggingBuildExecutor(
							buildExecutor,
							browserURL),
						tracerProvider)

					instanceNamePrefix, err := digest.NewInstanceName(runnerConfiguration.InstanceNamePrefix)
					if err != nil {
						return util.StatusWrapf(err, "Invalid instance name prefix %#v", runnerConfiguration.InstanceNamePrefix)
					}

					buildClient := builder.NewBuildClient(
						schedulerClient,
						buildExecutor,
						re_filesystem.NewQuotaEnforcingFilePool(
							filePool,
							runnerConfiguration.MaximumFilePoolFileCount,
							runnerConfiguration.MaximumFilePoolSizeBytes),
						clock.SystemClock,
						workerID,
						instanceNamePrefix,
						runnerConfiguration.Platform,
						runnerConfiguration.SizeClass)
					builder.LaunchWorkerThread(siblingsGroup, buildClient, string(workerName))
				}
			}
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
