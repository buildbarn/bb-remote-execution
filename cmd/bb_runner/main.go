package main

import (
	"log"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
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
	if err := global.ApplyConfiguration(configuration.Global); err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	buildDirectory, err := filesystem.NewLocalDirectory(configuration.BuildDirectoryPath)
	if err != nil {
		log.Fatal("Failed to open build directory: ", err)
	}

	r := runner.NewLocalRunner(
		buildDirectory,
		configuration.BuildDirectoryPath)

	// When temporary directories need cleaning prior to executing a build
	// action, attach a series of TemporaryDirectoryCleaningRunners.
	for _, d := range configuration.TemporaryDirectories {
		directory, err := filesystem.NewLocalDirectory(d)
		if err != nil {
			log.Fatalf("Failed to open temporary directory %#v: %s", d, err)
		}
		r = runner.NewTemporaryDirectoryCleaningRunner(r, directory, d)
	}

	log.Fatal(
		"gRPC server failure: ",
		bb_grpc.NewGRPCServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s *grpc.Server) {
				runner_pb.RegisterRunnerServer(s, runner.NewRunnerServer(r))
			}))
}
