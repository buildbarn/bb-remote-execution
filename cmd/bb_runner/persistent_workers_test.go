package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	grpc_configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
)

func TestPersistentRunnerConfiguration(test *testing.T) {
	testCases := []struct {
		name          string
		configuration *bb_runner.ApplicationConfiguration
		invalid       bool
	}{
		{name: "Disabled", configuration: &bb_runner.ApplicationConfiguration{SymlinkTemporaryDirectories: []string{"/tmp"}}},
		{name: "MissingSize", invalid: true, configuration: &bb_runner.ApplicationConfiguration{PersistentWorkers: &bb_runner.PersistentWorkersConfiguration{}}},
		{name: "Oversized", invalid: true, configuration: &bb_runner.ApplicationConfiguration{PersistentWorkers: &bb_runner.PersistentWorkersConfiguration{MaximumWorkResponseSizeBytes: math.MaxUint64}}},
		{name: "Symlink", invalid: true, configuration: &bb_runner.ApplicationConfiguration{
			PersistentWorkers: &bb_runner.PersistentWorkersConfiguration{MaximumWorkResponseSizeBytes: 1024}, SymlinkTemporaryDirectories: []string{"/tmp"},
		}},
		{name: "Installer", invalid: true, configuration: &bb_runner.ApplicationConfiguration{
			PersistentWorkers: &bb_runner.PersistentWorkersConfiguration{MaximumWorkResponseSizeBytes: 1024}, TemporaryDirectoryInstaller: &grpc_configuration.ClientConfiguration{},
		}},
	}

	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			server, closer, err := newPersistentRunnerFromConfiguration(context.Background(), testCase.configuration, nil, nil, nil, nil)
			require.Nil(test, server)
			require.Nil(test, closer)
			if testCase.invalid {
				require.Error(test, err)
			} else {
				require.NoError(test, err)
			}
		})
	}
}

func TestPersistentRunnerConfigurationEnabled(test *testing.T) {
	directoryPath := test.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(test, err)
	defer directory.Close()
	directoryBuilder, walker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(test, path.Resolve(path.LocalFormat.NewParser(directoryPath), walker))
	pathname := filepath.Join(directoryPath, "required")
	require.NoError(test, os.WriteFile(pathname, nil, 0o600))
	configuration := &bb_runner.ApplicationConfiguration{
		PersistentWorkers:          &bb_runner.PersistentWorkersConfiguration{MaximumWorkResponseSizeBytes: 1024},
		ReadinessCheckingPathnames: []string{pathname},
	}
	server, closer, err := newPersistentRunnerFromConfiguration(context.Background(), configuration, directory, directoryBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), nil)
	require.NoError(test, err)
	defer func() { require.NoError(test, closer.Close()) }()
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	runner_pb.RegisterPersistentRunnerServer(grpcServer, server)
	require.Contains(test, grpcServer.GetServiceInfo(), "buildbarn.runner.PersistentRunner")
	_, err = server.CheckReadiness(context.Background(), &runner_pb.CheckReadinessRequest{Path: "required"})
	require.NoError(test, err)
	require.NoError(test, os.Remove(pathname))
	_, err = server.CheckReadiness(context.Background(), &runner_pb.CheckReadinessRequest{Path: "required"})
	require.Error(test, err)
}
