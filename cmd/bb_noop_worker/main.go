package main

import (
	"log"
	"net/url"
	"os"
	"time"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_noop_worker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// This is an implementation of a remote execution worker that always
// fails to execute actions with an INVALID_ARGUMENT error. This worker
// may be useful when attempting to inspect input roots of actions, as
// it causes the client to print a link to bb_browser immediately.

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_noop_worker bb_noop_worker.jsonnet")
	}
	var configuration bb_noop_worker.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}
	terminationContext, terminationGroup := global.InstallGracefulTerminationHandler()

	// Storage access. This worker loads Command objects from the
	// Content Addressable Storage (CAS), as those may contain error
	// message templates that this worker respects.
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

	browserURL, err := url.Parse(configuration.BrowserUrl)
	if err != nil {
		log.Fatal("Failed to parse browser URL: ", err)
	}

	schedulerConnection, err := grpcClientFactory.NewClientFromConfiguration(configuration.Scheduler)
	if err != nil {
		log.Fatal("Failed to create scheduler RPC client: ", err)
	}
	schedulerClient := remoteworker.NewOperationQueueClient(schedulerConnection)

	instanceNamePrefix, err := digest.NewInstanceName(configuration.InstanceNamePrefix)
	if err != nil {
		log.Fatalf("Invalid instance name prefix %#v: %s", configuration.InstanceNamePrefix, err)
	}

	buildClient := builder.NewBuildClient(
		schedulerClient,
		builder.NewNoopBuildExecutor(
			contentAddressableStorage,
			int(configuration.MaximumMessageSizeBytes),
			browserURL),
		re_filesystem.EmptyFilePool,
		clock.SystemClock,
		configuration.WorkerId,
		instanceNamePrefix,
		configuration.Platform,
		0)

	go func() {
		generator := random.NewFastSingleThreadedGenerator()
		for {
			if err := buildClient.Run(); err != nil {
				log.Print(err)
				time.Sleep(random.Duration(generator, 5*time.Second))
			}
		}
	}()

	lifecycleState.MarkReadyAndWait()
}
