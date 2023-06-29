package main

import (
	"context"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_virtual_tmp"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This service provides a virtual file system that merely provides a
// symbolic link named "tmp". By implementing the tmp_installer gRPC
// service, bb_runner is capable of calling into this service to adjust
// the target of this symbolic link, so that it points to the temporary
// directory that bb_worker allocates for every action it runs.
//
// The "tmp" symbolic link is virtualized, meaning that the target
// differs depending on the credentials of the user accessing it. This
// makes it possible to give every build action its own /tmp, even if
// the host operating system does not offer file system namespace
// virtualization (containers/jails), or magic symlinks that evaluate to
// different locations based on the user ID.

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_virtual_tmp bb_virtual_tmp.jsonnet")
		}
		var configuration bb_virtual_tmp.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, _, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		// Create symbolic link whose target can be set by users.
		buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
		if err := path.Resolve(configuration.BuildDirectoryPath, scopeWalker); err != nil {
			return util.StatusWrap(err, "Failed to resolve build directory path")
		}
		userSettableSymlink := virtual.NewUserSettableSymlink(buildDirectory)

		// Expose the symbolic link through a virtual file system.
		mount, handleAllocator, err := virtual_configuration.NewMountFromConfiguration(
			configuration.Mount,
			"bb_virtual_tmp",
			/* containsSelfMutatingSymlinks = */ true)
		if err != nil {
			return util.StatusWrap(err, "Failed to create virtual file system mount")
		}
		if err := mount.Expose(
			siblingsGroup,
			handleAllocator.New().AsStatelessDirectory(
				virtual.NewStaticDirectory(map[path.Component]virtual.DirectoryChild{
					path.MustNewComponent("tmp"): virtual.DirectoryChild{}.
						FromLeaf(handleAllocator.New().AsNativeLeaf(userSettableSymlink)),
				}))); err != nil {
			return util.StatusWrap(err, "Failed to expose virtual file system mount")
		}

		// Allow users to set the target through gRPC.
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				tmp_installer.RegisterTemporaryDirectoryInstallerServer(s, userSettableSymlink)
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
