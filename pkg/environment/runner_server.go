package environment

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/protobuf/ptypes/empty"
)

type runnerServer struct {
	manager Manager
}

// NewRunnerServer returns a RunnerServer that runs every action in its
// own acquired Environment.
func NewRunnerServer(manager Manager) runner.RunnerServer {
	return &runnerServer{
		manager: manager,
	}
}

func (rs *runnerServer) CheckReadiness(ctx context.Context, request *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (rs *runnerServer) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	env, err := rs.manager.Acquire(digest.BadDigest)
	if err != nil {
		return nil, err
	}
	defer env.Release()
	return env.Run(ctx, request)
}
