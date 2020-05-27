package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_worker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_worker bb_worker.jsonnet")
	}
	var configuration bb_worker.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	if err := global.ApplyConfiguration(configuration.Global); err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
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
	globalContentAddressableStorageBlobAccess, actionCache, err := blobstore_configuration.CreateBlobAccessObjectsFromConfig(
		configuration.Blobstore,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}

	// Cached read access for directory objects stored in the
	// Content Addressable Storage. All workers make use of the same
	// cache, to increase the hit rate.
	globalContentAddressableStorage := re_cas.NewDirectoryCachingContentAddressableStorage(
		cas.NewBlobAccessContentAddressableStorage(
			re_blobstore.NewExistencePreconditionBlobAccess(globalContentAddressableStorageBlobAccess),
			int(configuration.MaximumMessageSizeBytes)),
		digest.KeyWithoutInstance,
		int(configuration.MaximumMemoryCachedDirectories),
		eviction.NewMetricsSet(eviction.NewRRSet(), "DirectoryCachingContentAddressableStorage"))

	if len(configuration.BuildDirectories) == 0 {
		log.Fatal("Cannot start worker without any build directories")
	}
	for _, buildDirectoryConfiguration := range configuration.BuildDirectories {
		var naiveBuildDirectory filesystem.DirectoryCloser
		var perBuildDirectoryContentAddressableStorage cas.ContentAddressableStorage
		switch backend := buildDirectoryConfiguration.Backend.(type) {
		case *bb_worker.BuildDirectoryConfiguration_Native:
			// To ease privilege separation, clear the umask. This
			// process either writes files into directories that can
			// easily be closed off, or creates files with the
			// appropriate mode to be secure.
			syscall.Umask(0)

			// Directory where actual builds take place.
			nativeConfiguration := backend.Native
			naiveBuildDirectory, err = filesystem.NewLocalDirectory(nativeConfiguration.BuildDirectoryPath)
			if err != nil {
				log.Fatal("Failed to open build directory: ", err)
			}

			// Create a cache directory that holds input
			// files that can be hardlinked into build
			// directory.
			//
			// TODO: Move ContentAddressableStorage.GetFile()
			// into its own FilePlacer interface that is
			// isolated to NaiveBuildDirectory. That would
			// prevent the need for all of this wrapping and
			// ReadWriteDecouplingContentAddressableStorage.
			//
			// TODO: Have a single process-wide hardlinking
			// cache even if multiple build directories are
			// used. This increases cache hit rate.
			cacheDirectory, err := filesystem.NewLocalDirectory(nativeConfiguration.CacheDirectoryPath)
			if err != nil {
				log.Fatal("Failed to open cache directory: ", err)
			}
			if err := cacheDirectory.RemoveAllChildren(); err != nil {
				log.Fatal("Failed to clear cache directory: ", err)
			}
			evictionSet, err := eviction.NewSetFromConfiguration(nativeConfiguration.CacheReplacementPolicy)
			if err != nil {
				log.Fatal("Failed to create eviction set for cache directory: ", err)
			}
			perBuildDirectoryContentAddressableStorage = re_cas.NewHardlinkingContentAddressableStorage(
				globalContentAddressableStorage,
				digest.KeyWithoutInstance,
				cacheDirectory,
				int(nativeConfiguration.MaximumCacheFileCount),
				nativeConfiguration.MaximumCacheSizeBytes,
				eviction.NewMetricsSet(evictionSet, "HardlinkingContentAddressableStorage"))
		default:
			log.Fatal("No build directory specified")
		}

		var buildDirectoryInitializer sync.Initializer
		var sharedBuildDirectoryNextParallelActionID uint64
		if len(buildDirectoryConfiguration.Runners) == 0 {
			log.Fatal("Cannot start worker without any runners")
		}
		for _, runnerConfiguration := range buildDirectoryConfiguration.Runners {
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

			inputRootCharacterDevices := make(map[string]int)
			for _, device := range runnerConfiguration.InputRootCharacterDeviceNodes {
				var stat unix.Stat_t
				devicePath := filepath.Join("/dev", device)
				if err := unix.Stat(devicePath, &stat); err != nil {
					log.Fatalf("Unable to stat character device %#v: %s", devicePath, err)
				}
				if stat.Mode&syscall.S_IFMT != syscall.S_IFCHR {
					log.Fatalf("The specified device %#v is not a character device", devicePath)
				}
				inputRootCharacterDevices[device] = int(stat.Rdev)
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

			// Wait for the runner process to come online.
			runnerClient := runner_pb.NewRunnerClient(runnerConnection)
			for {
				_, err := runnerClient.CheckReadiness(context.Background(), &empty.Empty{})
				if err == nil {
					break
				}
				log.Print("Runner is not ready yet: ", err)
				time.Sleep(3 * time.Second)
			}

			for threadID := uint64(0); threadID < runnerConfiguration.Concurrency; threadID++ {
				go func(runnerConfiguration *bb_worker.RunnerConfiguration, threadID uint64) {
					// Per-worker separate writer of the Content
					// Addressable Storage that batches writes after
					// completing the build action.
					contentAddressableStorageWriter, contentAddressableStorageFlusher := re_blobstore.NewBatchedStoreBlobAccess(
						re_blobstore.NewExistencePreconditionBlobAccess(globalContentAddressableStorageBlobAccess),
						digest.KeyWithoutInstance, 100)
					contentAddressableStorageWriter = blobstore.NewMetricsBlobAccess(
						contentAddressableStorageWriter,
						clock.SystemClock,
						"cas_batched_store")
					perThreadContentAddressableStorage := re_cas.NewReadWriteDecouplingContentAddressableStorage(
						perBuildDirectoryContentAddressableStorage,
						cas.NewBlobAccessContentAddressableStorage(
							contentAddressableStorageWriter,
							int(configuration.MaximumMessageSizeBytes)))

					buildDirectory := builder.NewNaiveBuildDirectory(
						naiveBuildDirectory,
						perThreadContentAddressableStorage)

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
							&buildDirectoryInitializer),
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
						log.Fatal("Failed to marshal worker ID: ", err)
					}

					buildExecutor := builder.NewLoggingBuildExecutor(
						builder.NewCachingBuildExecutor(
							builder.NewMetricsBuildExecutor(
								builder.NewFilePoolStatsBuildExecutor(
									builder.NewTimestampedBuildExecutor(
										builder.NewStorageFlushingBuildExecutor(
											builder.NewLocalBuildExecutor(
												perThreadContentAddressableStorage,
												buildDirectoryCreator,
												runner.NewRemoteRunner(runnerConnection),
												clock.SystemClock,
												defaultExecutionTimeout,
												maximumExecutionTimeout,
												inputRootCharacterDevices),
											contentAddressableStorageFlusher),
										clock.SystemClock,
										string(workerName)))),
							cas.NewBlobAccessContentAddressableStorage(
								globalContentAddressableStorageBlobAccess,
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
	}

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
