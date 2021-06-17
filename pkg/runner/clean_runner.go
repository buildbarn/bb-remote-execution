package runner

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/protobuf/types/known/emptypb"
)

type cleanRunner struct {
	base        runner_pb.RunnerServer
	idleInvoker *cleaner.IdleInvoker
}

// NewCleanRunner is a decorator for Runner that calls into an
// IdleInvoker before and after running a build action.
//
// This decorator can be used to run cleanup tasks that are needed to be
// performed to bring the execution environment in a consistent state
// (e.g., scrubbing the process table, removing stale temporary files).
func NewCleanRunner(base runner_pb.RunnerServer, idleInvoker *cleaner.IdleInvoker) runner_pb.RunnerServer {
	return &cleanRunner{
		base:        base,
		idleInvoker: idleInvoker,
	}
}

func (r *cleanRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if err := r.idleInvoker.Acquire(ctx); err != nil {
		return nil, err
	}
	response, err1 := r.base.Run(ctx, request)
	err2 := r.idleInvoker.Release(ctx)
	if err1 != nil {
		return nil, err1
	}
	return response, err2
}

func (r *cleanRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	if err := r.idleInvoker.Acquire(ctx); err != nil {
		return nil, err
	}
	response, err1 := r.base.CheckReadiness(ctx, request)
	err2 := r.idleInvoker.Release(ctx)
	if err1 != nil {
		return nil, err1
	}
	return response, err2
}
