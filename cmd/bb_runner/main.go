package main

import (
	"flag"
	"log"
	"net"
	"os"

	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
)

func main() {
	var tempDirectoriesList util.StringList
	var (
		buildDirectoryPath = flag.String("build-directory", "/worker/build", "Directory where builds take place")
		listenPath         = flag.String("listen-path", "/worker/runner", "Path on which this process should bind its UNIX socket to wait for incoming requests through GRPC")
	)
	flag.Var(&tempDirectoriesList, "temp-directory", "Temporary directory that should be cleaned up after a build action. Example: /tmp")
	flag.Parse()

	buildDirectory, err := filesystem.NewLocalDirectory(*buildDirectoryPath)
	if err != nil {
		log.Fatal("Failed to open build directory: ", err)
	}

	env := environment.NewLocalExecutionEnvironment(buildDirectory, *buildDirectoryPath)
	var runnerServer runner.RunnerServer
	// When temporary directories need cleaning prior to executing a build
	// action, attach a series of TempDirectoryCleaningManagers.
	if len(tempDirectoriesList) > 0 {
		m := environment.NewSingletonManager(env)
		for _, d := range tempDirectoriesList {
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

	if err := os.Remove(*listenPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Could not remove stale socket %#v: %s", *listenPath, err)
	}

	sock, err := net.Listen("unix", *listenPath)
	if err != nil {
		log.Fatalf("Failed to create listening socket %#v: %s", *listenPath, err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
