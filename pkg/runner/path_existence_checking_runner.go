package runner

import (
	"context"
	"os"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

type pathExistenceCheckingRunner struct {
	base                       runner_pb.RunnerServer
	readinessCheckingPathnames []string
}

// NewPathExistenceCheckingRunner creates a decorator of RunnerServer
// that is only healthy when certain paths on disk exist.
func NewPathExistenceCheckingRunner(base runner_pb.RunnerServer, readinessCheckingPathnames []string) runner_pb.RunnerServer {
	return &pathExistenceCheckingRunner{
		base:                       base,
		readinessCheckingPathnames: readinessCheckingPathnames,
	}
}

func (r *pathExistenceCheckingRunner) checkPathExistence(ctx context.Context) error {
	for _, path := range r.readinessCheckingPathnames {
		if _, err := os.Stat(path); err != nil {
			return util.StatusWrapfWithCode(err, codes.Unavailable, "Path %#v", path)
		}
	}
	return nil
}

func (r *pathExistenceCheckingRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	if err := r.checkPathExistence(ctx); err != nil {
		return nil, err
	}
	return r.base.CheckReadiness(ctx, request)
}

func (r *pathExistenceCheckingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	response, err := r.base.Run(ctx, request)
	if err != nil {
		return nil, err
	}
	if response.ExitCode != 0 {
		// Execution failues may be caused by files
		// disappearing. Suppress the results in case the
		// readiness check fails.
		if err := r.checkPathExistence(ctx); err != nil {
			return nil, util.StatusWrap(err, "One or more required files disappeared during execution")
		}
	}
	return response, nil
}
