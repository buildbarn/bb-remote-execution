package main

import (
	"context"
	"os"
	"sort"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_virtual_dir"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bb_virtual_dir is a standalone utility that mounts a virtual file system
// at a specified path in much the same way as bb_worker does. It is
// primarily intended for testing purposes. Once the folder is mounted,
// the folder can be manipulated using normal system calls. Any data
// stored in the virtual file system will be persisted in the configured
// backing store which can either be on disk, or in-memory (in which case
// the virtual directory's contents will be removed on shutdown).

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_virtual_dir bb_virtual_dir.jsonnet")
		}
		var configuration bb_virtual_dir.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, _, err := global.ApplyConfiguration(configuration.Global, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		mount, handleAllocator, err := virtual_configuration.NewMountFromConfiguration(
			configuration.Mount,
			"bb_virtual_dir",
			/* rootDirectory = */ virtual_configuration.LongAttributeCaching,
			/* childDirectories = */ virtual_configuration.LongAttributeCaching,
			/* leaves = */ virtual_configuration.NoAttributeCaching)
		if err != nil {
			return util.StatusWrap(err, "Failed to create virtual file system mount")
		}
		initialContentsSorter := sort.Sort
		hiddenFilesPattern := func(s string) bool { return false }

		filePool, err := pool.NewFilePoolFromConfiguration(
			configuration.FilePool,
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create file pool")
		}

		if err := mount.Expose(
			siblingsGroup,
			virtual.NewInMemoryPrepopulatedDirectory(
				virtual.NewHandleAllocatingFileAllocator(
					virtual.NewPoolBackedFileAllocator(
						filePool,
						util.DefaultErrorLogger),
					handleAllocator),
				virtual.NewHandleAllocatingSymlinkFactory(
					virtual.BaseSymlinkFactory,
					handleAllocator.New()),
				util.DefaultErrorLogger,
				handleAllocator,
				initialContentsSorter,
				hiddenFilesPattern,
				clock.SystemClock,
				mount.CaseSensitive(),
				/* defaultAttributesSetter = */ func(requested virtual.AttributesMask, attributes *virtual.Attributes) {
					attributes.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead)
				})); err != nil {
			return util.StatusWrap(err, "Failed to expose virtual file system mount")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
