package runner

import (
	"context"
	"os"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

type runnerServer struct {
	runner                     Runner
	readinessCheckingPathnames []string
}

// NewRunnerServer returns a gRPC RunnerServer that forwards every call
// to a plain Runner. It also supports a simple form of readiness
// checking, ensuring that a runner is only healthy when certain paths
// on disk exist.
func NewRunnerServer(runner Runner, readinessCheckingPathnames []string) runner_pb.RunnerServer {
	return &runnerServer{
		runner:                     runner,
		readinessCheckingPathnames: readinessCheckingPathnames,
	}
}

func (rs *runnerServer) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	for _, path := range rs.readinessCheckingPathnames {
		if _, err := os.Stat(path); err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Unavailable, "Path %#v", path)
		}
	}
	return &emptypb.Empty{}, nil
}

func (rs *runnerServer) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	response, err := rs.runner.Run(ctx, request)
	if err != nil {
		return nil, err
	}
	if response.ExitCode != 0 {
		// Execution failues may be caused by failing readiness
		// checks. Suppress the results in case the readiness
		// check fails.
		if _, err := rs.CheckReadiness(ctx, &emptypb.Empty{}); err != nil {
			return nil, util.StatusWrap(err, "Readiness check failed during execution")
		}
	}
	return response, nil
}
