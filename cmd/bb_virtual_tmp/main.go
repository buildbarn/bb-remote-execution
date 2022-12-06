package main

import (
	"log"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_virtual_tmp"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
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
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_virtual_tmp bb_virtual_tmp.jsonnet")
	}
	var configuration bb_virtual_tmp.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, _, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}
	terminationContext, terminationGroup := global.InstallGracefulTerminationHandler()

	// Create symbolic link whose target can be set by users.
	buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(configuration.BuildDirectoryPath, scopeWalker); err != nil {
		log.Fatal("Failed to resolve build directory path: ", err)
	}
	userSettableSymlink := virtual.NewUserSettableSymlink(buildDirectory)

	// Expose the symbolic link through a virtual file system.
	mount, handleAllocator, err := virtual_configuration.NewMountFromConfiguration(
		configuration.Mount,
		"bb_virtual_tmp",
		/* containsSelfMutatingSymlinks = */ true)
	if err != nil {
		log.Fatal("Failed to create virtual file system mount: ", err)
	}
	if err := mount.Expose(
		terminationContext,
		terminationGroup,
		handleAllocator.New().AsStatelessDirectory(
			virtual.NewStaticDirectory(map[path.Component]virtual.DirectoryChild{
				path.MustNewComponent("tmp"): virtual.DirectoryChild{}.
					FromLeaf(handleAllocator.New().AsNativeLeaf(userSettableSymlink)),
			}))); err != nil {
		log.Fatal("Failed to expose virtual file system mount: ", err)
	}

	// Allow users to set the target through gRPC.
	if err := bb_grpc.NewServersFromConfigurationAndServe(
		configuration.GrpcServers,
		func(s grpc.ServiceRegistrar) {
			tmp_installer.RegisterTemporaryDirectoryInstallerServer(s, userSettableSymlink)
		}); err != nil {
		log.Fatal("gRPC server failure: ", err)
	}

	lifecycleState.MarkReadyAndWait()
}
