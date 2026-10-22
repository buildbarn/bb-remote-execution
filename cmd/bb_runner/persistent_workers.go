package main

import (
	"context"
	"math"
	"runtime"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newPersistentRunnerFromConfiguration(ctx context.Context, configuration *bb_runner.ApplicationConfiguration, directory filesystem.Directory, directoryPath *path.Builder, commandCreator runner.CommandCreator, idleInvoker *cleaner.IdleInvoker) (runner_pb.PersistentRunnerServer, *runner.PersistentRunner, error) {
	persistent := configuration.PersistentWorkers
	if persistent == nil {
		return nil, nil, nil
	}

	if persistent.MaximumWorkResponseSizeBytes == 0 || persistent.MaximumWorkResponseSizeBytes > math.MaxInt {
		return nil, nil, status.Error(codes.InvalidArgument, "Persistent worker response size must be positive and fit in an int")
	}
	if len(configuration.SymlinkTemporaryDirectories) > 0 || configuration.TemporaryDirectoryInstaller != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "Persistent workers cannot use global temporary directory rewrites; use set_tmpdir_environment_variable instead")
	}

	server := runner.NewPersistentRunner(ctx, directory, directoryPath, commandCreator, configuration.SetTmpdirEnvironmentVariable, persistent.MaximumWorkResponseSizeBytes, idleInvoker)
	var decorated runner_pb.PersistentRunnerServer = server
	if len(configuration.ReadinessCheckingPathnames) > 0 {
		decorated = runner.NewPathExistenceCheckingPersistentRunner(decorated, configuration.ReadinessCheckingPathnames)
	}

	if runtime.GOOS == "darwin" {
		decorated = runner.NewAppleXcodeResolvingPersistentRunner(decorated, configuration.AppleXcodeDeveloperDirectories,
			runner.NewCachingAppleXcodeSDKRootResolver(runner.LocalAppleXcodeSDKRootResolver))
	}

	return decorated, server, nil
}
