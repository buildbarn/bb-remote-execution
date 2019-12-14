package main

import (
	"log"
	"net"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
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
	var runnerServer runner.RunnerServer
	// When temporary directories need cleaning prior to executing a build
	// action, attach a series of TempDirectoryCleaningManagers.
	if len(configuration.TemporaryDirectories) > 0 {
		m := environment.NewSingletonManager(env)
		for _, d := range configuration.TemporaryDirectories {
			directory, err := filesystem.NewLocalDirectory(d)
			if err != nil {
				log.Fatalf("Failed to open temporary directory %#v: %s", d, err)
			}
			m = environment.NewTempDirectoryCleaningManager(m, directory)
		}
		runnerServer = environment.NewRunnerServer(environment.NewConcurrentManager(m))
	} else {
		runnerServer = env
	}

	s := grpc.NewServer()
	runner.RegisterRunnerServer(s, runnerServer)

	if err := os.Remove(configuration.ListenPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Could not remove stale socket %#v: %s", configuration.ListenPath, err)
	}

	sock, err := net.Listen("unix", configuration.ListenPath)
	if err != nil {
		log.Fatalf("Failed to create listening socket %#v: %s", configuration.ListenPath, err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
