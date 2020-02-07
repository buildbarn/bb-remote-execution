package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"syscall"
	"time"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_worker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/gorilla/mux"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_worker bb_worker.jsonnet")
	}
	var configuration bb_worker.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	browserURL, err := url.Parse(configuration.BrowserUrl)
	if err != nil {
		log.Fatal("Failed to parse browser URL: ", err)
	}

	// Create connection with scheduler.
	schedulerConnection, err := bb_grpc.NewGRPCClientFromConfiguration(configuration.Scheduler)
	if err != nil {
		log.Fatal("Failed to create scheduler RPC client: ", err)
	}
	schedulerClient := remoteworker.NewOperationQueueClient(schedulerConnection)

	// Location for storing temporary file objects.
	var filePool re_filesystem.FilePool
	if configuration.FilePoolDirectoryPath == "" {
		filePool = re_filesystem.NewInMemoryFilePool()
	} else {
		filePoolDirectory, err := filesystem.NewLocalDirectory(configuration.FilePoolDirectoryPath)
		if err != nil {
			log.Fatal("Failed to open output directory: ", err)
		}
		if err := filePoolDirectory.RemoveAllChildren(); err != nil {
			log.Fatal("Failed to empty out output directory: ", err)
		}
		filePool = re_filesystem.NewDirectoryBackedFilePool(filePoolDirectory)
	}

	// Storage access.
	contentAddressableStorageBlobAccess, actionCache, err := blobstore_configuration.CreateBlobAccessObjectsFromConfig(
		configuration.Blobstore,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}

	var buildDirectory filesystem.Directory
	switch buildDirectoryConfigurationVariant := configuration.BuildDirectory.(type) {
	case *bb_worker.ApplicationConfiguration_LocalBuildDirectory:
		// To ease privilege separation, clear the umask. This
		// process either writes files into directories that can
		// easily be closed off, or creates files with the
		// appropriate mode to be secure.
		syscall.Umask(0)

		// Directory where actual builds take place.
		buildDirectoryConfiguration := buildDirectoryConfigurationVariant.LocalBuildDirectory
		buildDirectory, err = filesystem.NewLocalDirectory(buildDirectoryConfiguration.BuildDirectoryPath)
		if err != nil {
			log.Fatal("Failed to open build directory: ", err)
		}
		// TODO: This may be removed when the
		// CleanBuildDirectoryManager is enabled unconditionally
		// once again.
		if err := buildDirectory.RemoveAllChildren(); err != nil {
			log.Fatal("Failed to clean build directory on startup: ", err)
		}
	default:
		log.Fatal("No build directory specified")
	}

	// Cached read access for directory objects stored in the
	// Content Addressable Storage. All workers make use of the same
	// cache, to increase the hit rate.
	contentAddressableStorageReader := re_cas.NewDirectoryCachingContentAddressableStorage(
		cas.NewBlobAccessContentAddressableStorage(
			re_blobstore.NewExistencePreconditionBlobAccess(contentAddressableStorageBlobAccess),
			int(configuration.MaximumMessageSizeBytes)),
		digest.KeyWithoutInstance,
		int(configuration.MaximumMemoryCachedDirectories),
		eviction.NewMetricsSet(eviction.NewRRSet(), "DirectoryCachingContentAddressableStorage"))

	// Create a cache directory that holds input files that can be
	// hardlinked into build directory.
	switch buildDirectoryConfigurationVariant := configuration.BuildDirectory.(type) {
	case *bb_worker.ApplicationConfiguration_LocalBuildDirectory:
		buildDirectoryConfiguration := buildDirectoryConfigurationVariant.LocalBuildDirectory
		cacheDirectory, err := filesystem.NewLocalDirectory(buildDirectoryConfiguration.CacheDirectoryPath)
		if err != nil {
			log.Fatal("Failed to open cache directory: ", err)
		}
		if err := cacheDirectory.RemoveAllChildren(); err != nil {
			log.Fatal("Failed to clear cache directory: ", err)
		}
		evictionSet, err := eviction.NewSetFromConfiguration(buildDirectoryConfiguration.CacheReplacementPolicy)
		if err != nil {
			log.Fatal("Failed to create eviction set for cache directory: ", err)
		}
		contentAddressableStorageReader = re_cas.NewHardlinkingContentAddressableStorage(
			contentAddressableStorageReader,
			digest.KeyWithoutInstance,
			cacheDirectory,
			int(buildDirectoryConfiguration.MaximumCacheFileCount),
			buildDirectoryConfiguration.MaximumCacheSizeBytes,
			eviction.NewMetricsSet(evictionSet, "HardlinkingContentAddressableStorage"))
	}

	if len(configuration.Runners) == 0 {
		log.Fatal("Cannot start worker without any runners")
	}
	for _, runnerConfiguration := range configuration.Runners {
		if runnerConfiguration.Concurrency < 1 {
			log.Fatal("Runner concurrency must be positive")
		}
		concurrencyLength := len(strconv.FormatUint(runnerConfiguration.Concurrency-1, 10))

		defaultExecutionTimeout, err := ptypes.Duration(runnerConfiguration.DefaultExecutionTimeout)
		if err != nil {
			log.Fatal("Failed to parse default execution timeout")
		}
		maximumExecutionTimeout, err := ptypes.Duration(runnerConfiguration.MaximumExecutionTimeout)
		if err != nil {
			log.Fatal("Failed to parse maximum execution timeout")
		}

		// Execute commands using a separate runner process. Due to the
		// interaction between threads, forking and execve() returning
		// ETXTBSY, concurrent execution of build actions can only be
		// used in combination with a runner process. Having a separate
		// runner process also makes it possible to apply privilege
		// separation.
		runnerConnection, err := bb_grpc.NewGRPCClientFromConfiguration(runnerConfiguration.Endpoint)
		if err != nil {
			log.Fatal("Failed to create runner RPC client: ", err)
		}

		// Build environment capable of executing one action at a time.
		// The build takes place in the root of the build directory.
		environmentManager := environment.NewSingletonManager(
			environment.NewRemoteExecutionEnvironment(runnerConnection, buildDirectory))

		// Clean the build directory every time when going from
		// fully idle to executing one action.
		// TODO: Also enable this feature when multiple runners
		// are configured. Being able to support this requires a
		// decomposition of environment.Environment into two
		// separate interfaces: one for managing the build
		// directory and one for running commands.
		if len(configuration.Runners) == 1 {
			environmentManager = environment.NewCleanBuildDirectoryManager(environmentManager)
		}

		// Create a per-action subdirectory in the build directory named
		// after the action digest, so that multiple actions may be run
		// concurrently within the same environment.
		// TODO(edsch): It might make sense to disable this if
		// concurrency is disabled to improve action cache hit rate, but
		// only if there are no other workers in the same cluster that
		// have concurrency enabled.
		environmentManager = environment.NewActionDigestSubdirectoryManager(
			environment.NewConcurrentManager(environmentManager))

		for threadID := uint64(0); threadID < runnerConfiguration.Concurrency; threadID++ {
			go func(runnerConfiguration *bb_worker.RunnerConfiguration, threadID uint64) {
				// Per-worker separate writer of the Content
				// Addressable Storage that batches writes after
				// completing the build action.
				contentAddressableStorageWriter, contentAddressableStorageFlusher := re_blobstore.NewBatchedStoreBlobAccess(
					re_blobstore.NewExistencePreconditionBlobAccess(contentAddressableStorageBlobAccess),
					digest.KeyWithoutInstance, 100)
				contentAddressableStorageWriter = blobstore.NewMetricsBlobAccess(
					contentAddressableStorageWriter,
					clock.SystemClock,
					"cas_batched_store")
				contentAddressableStorage := re_cas.NewReadWriteDecouplingContentAddressableStorage(
					contentAddressableStorageReader,
					cas.NewBlobAccessContentAddressableStorage(
						contentAddressableStorageWriter,
						int(configuration.MaximumMessageSizeBytes)))

				var inputRootPopulator builder.InputRootPopulator
				switch configuration.BuildDirectory.(type) {
				case *bb_worker.ApplicationConfiguration_LocalBuildDirectory:
					inputRootPopulator = builder.NewNaiveInputRootPopulator(
						contentAddressableStorage)
				}

				workerID := map[string]string{}
				if runnerConfiguration.Concurrency > 1 {
					workerID["thread"] = fmt.Sprintf("%0*d", concurrencyLength, threadID)
				}
				for k, v := range runnerConfiguration.WorkerId {
					workerID[k] = v
				}
				workerName, err := json.Marshal(workerID)
				if err != nil {
					log.Fatal("Failed to marshal worker ID: ", err)
				}

				buildExecutor := builder.NewLoggingBuildExecutor(
					builder.NewCachingBuildExecutor(
						builder.NewMetricsBuildExecutor(
							builder.NewFilePoolStatsBuildExecutor(
								builder.NewTimestampedBuildExecutor(
									builder.NewStorageFlushingBuildExecutor(
										builder.NewLocalBuildExecutor(
											contentAddressableStorage,
											environmentManager,
											inputRootPopulator,
											clock.SystemClock,
											defaultExecutionTimeout,
											maximumExecutionTimeout),
										contentAddressableStorageFlusher),
									clock.SystemClock,
									string(workerName)))),
						cas.NewBlobAccessContentAddressableStorage(
							contentAddressableStorageBlobAccess,
							int(configuration.MaximumMessageSizeBytes)),
						actionCache,
						browserURL),
					browserURL)

				buildClient := builder.NewBuildClient(
					schedulerClient,
					buildExecutor,
					re_filesystem.NewQuotaEnforcingFilePool(
						filePool,
						runnerConfiguration.MaximumFilePoolFileCount,
						runnerConfiguration.MaximumFilePoolSizeBytes),
					clock.SystemClock,
					browserURL,
					workerID,
					configuration.InstanceName,
					runnerConfiguration.Platform)
				for {
					if err := buildClient.Run(); err != nil {
						log.Print(err)
						time.Sleep(3 * time.Second)
					}
				}
			}(runnerConfiguration, threadID)
		}
	}

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
