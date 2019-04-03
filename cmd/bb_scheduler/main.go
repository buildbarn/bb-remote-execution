package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/configuration/bb_scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/scheduler"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_scheduler bb_scheduler.json")
	}

	schedulerConfiguration, err := configuration.GetSchedulerConfiguration(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(schedulerConfiguration.MetricsListenAddress, nil))
	}()

	var execDigestFunc remoteexecution.DigestFunction
	switch schedulerConfiguration.ExecutionDigestFunction {
	case "sha256":
		execDigestFunc = remoteexecution.DigestFunction_SHA256
	case "sha1":
		execDigestFunc = remoteexecution.DigestFunction_SHA1
	case "md5":
		execDigestFunc = remoteexecution.DigestFunction_MD5
	default:
		log.Fatalf("Unknown digest function '%s'", schedulerConfiguration.ExecutionDigestFunction)
	}

	executionServer, schedulerServer := builder.NewWorkerBuildQueue(
		util.DigestKeyWithInstance, execDigestFunc, schedulerConfiguration.JobsPendingMax)

	// RPC server.
	s := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	remoteexecution.RegisterCapabilitiesServer(s, executionServer)
	remoteexecution.RegisterExecutionServer(s, executionServer)
	scheduler.RegisterSchedulerServer(s, schedulerServer)
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(s)

	sock, err := net.Listen("tcp", schedulerConfiguration.GrpcListenAddress)
	if err != nil {
		log.Fatal("Failed to create listening socket: ", err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
