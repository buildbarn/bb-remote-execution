package main

import (
	"log"
	"os"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-remote-execution/pkg/credentials"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
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

	// Let bb_runner replace temporary directories with symbolic
	// links pointing to the temporary directory set up by
	// bb_worker.
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
		r = runner.NewTemporaryDirectoryInstallingRunner(r, tmpInstaller)
	}

	// Kill processes that actions leave behind by daemonizing.
	// Ensure that we only match processes belonging to the current
	// user that were created after bb_runner is spawned, as we
	// don't want to kill unrelated processes.
	var cleaners []cleaner.Cleaner
	if configuration.CleanProcessTable {
		startupTime := time.Now()
		cleaners = append(
			cleaners,
			cleaner.NewProcessTableCleaner(
				cleaner.NewFilteringProcessTable(
					cleaner.SystemProcessTable,
					func(process *cleaner.Process) bool {
						return process.UserID == processTableCleaningUserID &&
							process.CreationTime.After(startupTime)
					})))
	}

	// Clean temporary directories, so that files left behind by
	// build actions aren't visible to successive actions. This also
	// prevents systems from running out of disk space.
	for _, d := range configuration.CleanTemporaryDirectories {
		directory, err := filesystem.NewLocalDirectory(d)
		if err != nil {
			log.Fatalf("Failed to open temporary directory %#v: %s", d, err)
		}
		cleaners = append(cleaners, cleaner.NewDirectoryCleaner(directory, d))
	}
	if len(cleaners) > 0 {
		r = runner.NewCleanRunner(
			r,
			cleaner.NewIdleInvoker(cleaner.NewChainedCleaner(cleaners)))
	}

	// Paths that need to be present for the worker to be healthy.
	if len(configuration.ReadinessCheckingPathnames) > 0 {
		r = runner.NewPathExistenceCheckingRunner(r, configuration.ReadinessCheckingPathnames)
	}

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s grpc.ServiceRegistrar) {
					runner_pb.RegisterRunnerServer(s, r)
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}
