package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
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

	browserURL, err := url.Parse(configuration.BrowserUrl)
	if err != nil {
		log.Fatal("Failed to parse browser URL: ", err)
	}

	// Storage access. The scheduler requires access to the Action
	// and Command messages stored in the CAS to obtain platform
	// properties.
	contentAddressableStorageBlobAccess, err := blobstore_configuration.CreateCASBlobAccessObjectFromConfig(
		configuration.ContentAddressableStorage,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}
	contentAddressableStorage :=
		cas.NewBlobAccessContentAddressableStorage(
			re_blobstore.NewExistencePreconditionBlobAccess(
				contentAddressableStorageBlobAccess),
			16*1024*1024)

	// TODO: Make timeouts configurable.
	buildQueue := builder.NewInMemoryBuildQueue(
		contentAddressableStorage,
		clock.SystemClock,
		uuid.NewRandom,
		&builder.InMemoryBuildQueueConfiguration{
			ExecutionUpdateInterval:             time.Minute,
			OperationWithNoWaitersTimeout:       time.Minute,
			PlatformQueueWithNoWorkersTimeout:   15 * time.Minute,
			BusyWorkerSynchronizationInterval:   10 * time.Second,
			IdleWorkerSynchronizationInterval:   time.Minute,
			WorkerOperationRetryCount:           9,
			WorkerWithNoSynchronizationsTimeout: time.Minute,
		})

	// Spawn gRPC servers for client and worker traffic.
	go func() {
		log.Fatal(
			"Client gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.ClientGrpcServers,
				func(s *grpc.Server) {
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()
	go func() {
		log.Fatal(
			"Worker gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.WorkerGrpcServers,
				func(s *grpc.Server) {
					remoteworker.RegisterOperationQueueServer(s, buildQueue)
				}))
	}()

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	newBuildQueueStateService(buildQueue, clock.SystemClock, browserURL, router)
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
