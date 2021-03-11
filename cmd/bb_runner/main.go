package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/credentials"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	ptc "github.com/buildbarn/bb-remote-execution/pkg/runner/processtablecleaning"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_runner bb_runner.jsonnet")
	}
	var configuration bb_runner.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	buildDirectory := re_filesystem.NewLazyDirectory(
		func() (filesystem.DirectoryCloser, error) {
			return filesystem.NewLocalDirectory(configuration.BuildDirectoryPath)
		})
	buildDirectoryPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(configuration.BuildDirectoryPath, scopeWalker); err != nil {
		log.Fatal("Failed to resolve build directory: ", err)
	}

	sysProcAttr, processTableCleaningUserID, err := credentials.GetSysProcAttrFromConfiguration(configuration.RunCommandsAs)
	if err != nil {
		log.Fatal("Failed to extract credentials from configuration: ", err)
	}

	r := runner.NewLocalRunner(
		buildDirectory,
		buildDirectoryPath,
		sysProcAttr,
		configuration.SetTmpdirEnvironmentVariable,
		configuration.ChrootIntoInputRoot)

	// When temporary directories need cleaning prior to executing a build
	// action, attach a series of TemporaryDirectoryCleaningRunners.
	// Also wrap the Runner for every temporary directory that needs
	// to be symlinked.
	for _, d := range configuration.CleanTemporaryDirectories {
		directory, err := filesystem.NewLocalDirectory(d)
		if err != nil {
			log.Fatalf("Failed to open temporary directory %#v: %s", d, err)
		}
		r = runner.NewTemporaryDirectoryCleaningRunner(r, directory, d)
	}
	for _, symlinkPath := range configuration.SymlinkTemporaryDirectories {
		r = runner.NewTemporaryDirectorySymlinkingRunner(r, symlinkPath, buildDirectoryPath)
	}

	// Calling into a helper process to set up access to temporary
	// directories prior to the execution of build actions.
	if configuration.TemporaryDirectoryInstaller != nil {
		tmpInstallerConnection, err := bb_grpc.DefaultClientFactory.NewClientFromConfiguration(configuration.TemporaryDirectoryInstaller)
		if err != nil {
			log.Fatal("Failed to create temporary directory installer RPC client: ", err)
		}
		tmpInstaller := tmp_installer.NewTemporaryDirectoryInstallerClient(tmpInstallerConnection)
		for {
			_, err := tmpInstaller.CheckReadiness(context.Background(), &emptypb.Empty{})
			if err == nil {
				break
			}
			log.Print("Temporary directory installer is not ready yet: ", err)
			time.Sleep(3 * time.Second)
		}
		r = runner.NewTemporaryDirectoryInstallingRunner(r, tmpInstaller)
	}

	// Kill processes that actions leave behind by daemonizing.
	// Ensure that we only match processes belonging to the current
	// user that were created after bb_runner is spawned, as we
	// don't want to kill unrelated processes.
	if configuration.CleanProcessTable {
		startupTime := time.Now()
		r = ptc.NewProcessTableCleaningRunner(
			r,
			ptc.NewFilteringProcessTable(
				ptc.SystemProcessTable,
				func(process *ptc.Process) bool {
					return process.UserID == processTableCleaningUserID &&
						process.CreationTime.After(startupTime)
				}))
	}

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s *grpc.Server) {
					runner_pb.RegisterRunnerServer(
						s,
						runner.NewRunnerServer(
							r,
							configuration.ReadinessCheckingPathnames))
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}
