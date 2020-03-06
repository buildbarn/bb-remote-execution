package main

import (
	"log"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
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

	buildDirectory, err := filesystem.NewLocalDirectory(configuration.BuildDirectoryPath)
	if err != nil {
		log.Fatal("Failed to open build directory: ", err)
	}

	env := environment.NewLocalExecutionEnvironment(buildDirectory, configuration.BuildDirectoryPath)
	// When temporary directories need cleaning prior to executing a build
	// action, attach a series of TempDirectoryCleaningManagers.
	m := environment.NewSingletonManager(env)
	for _, d := range configuration.TemporaryDirectories {
		directory, err := filesystem.NewLocalDirectory(d)
		if err != nil {
			log.Fatalf("Failed to open temporary directory %#v: %s", d, err)
		}
		m = environment.NewTempDirectoryCleaningManager(m, directory)
	}
	runnerServer := environment.NewRunnerServer(environment.NewConcurrentManager(m))

	log.Fatal(
		"gRPC server failure: ",
		bb_grpc.NewGRPCServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s *grpc.Server) {
				runner.RegisterRunnerServer(s, runnerServer)
			}))
}
