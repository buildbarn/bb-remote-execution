package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/golang/protobuf/ptypes/empty"
)

type runnerServer struct {
	runner Runner
}

// NewRunnerServer returns a gRPC RunnerServer that forwards every call
// to a plain Runner.
func NewRunnerServer(runner Runner) runner_pb.RunnerServer {
	return &runnerServer{
		runner: runner,
	}
}

func (rs *runnerServer) CheckReadiness(ctx context.Context, request *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (rs *runnerServer) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	return rs.runner.Run(ctx, request)
}
