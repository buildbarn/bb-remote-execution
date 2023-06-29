package main

import (
	"context"
	"net/url"
	"os"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_noop_worker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This is an implementation of a remote execution worker that always
// fails to execute actions with an INVALID_ARGUMENT error. This worker
// may be useful when attempting to inspect input roots of actions, as
// it causes the client to print a link to bb_browser immediately.

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_noop_worker bb_noop_worker.jsonnet")
		}
		var configuration bb_noop_worker.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		// Storage access. This worker loads Command objects from the
		// Content Addressable Storage (CAS), as those may contain error
		// message templates that this worker respects.
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

		browserURL, err := url.Parse(configuration.BrowserUrl)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse browser URL")
		}

		schedulerConnection, err := grpcClientFactory.NewClientFromConfiguration(configuration.Scheduler)
		if err != nil {
			return util.StatusWrap(err, "Failed to create scheduler RPC client")
		}
		schedulerClient := remoteworker.NewOperationQueueClient(schedulerConnection)

		instanceNamePrefix, err := digest.NewInstanceName(configuration.InstanceNamePrefix)
		if err != nil {
			return util.StatusWrapf(err, "Invalid instance name prefix %#v", configuration.InstanceNamePrefix)
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
		builder.LaunchWorkerThread(siblingsGroup, buildClient, "noop")

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
