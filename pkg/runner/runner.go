package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
)

// Runner of build actions. LocalBuildExecutor calls into this interface
// to spawn the process described in the Command message.
type Runner interface {
	Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error)
}
