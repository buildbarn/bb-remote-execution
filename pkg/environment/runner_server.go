package environment

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
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

func (rs *runnerServer) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	env, err := rs.manager.Acquire(digest.BadDigest)
	if err != nil {
		return nil, err
	}
	defer env.Release()
	return env.Run(ctx, request)
}
