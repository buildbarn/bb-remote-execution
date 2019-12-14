package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/scheduler"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_scheduler bb_scheduler.jsonnet")
	}
	var configuration bb_scheduler.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	executionServer, schedulerServer := builder.NewWorkerBuildQueue(util.DigestKeyWithInstance, configuration.JobsPendingMax)

	// Spawn gRPC servers for client and worker traffic.
	go func() {
		log.Fatal(
			"Client gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.ClientGrpcServers,
				func(s *grpc.Server) {
					remoteexecution.RegisterCapabilitiesServer(s, executionServer)
					remoteexecution.RegisterExecutionServer(s, executionServer)
				}))
	}()
	go func() {
		log.Fatal(
			"Worker gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.WorkerGrpcServers,
				func(s *grpc.Server) {
					scheduler.RegisterSchedulerServer(s, schedulerServer)
				}))
	}()

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(configuration.MetricsListenAddress, nil))
}
