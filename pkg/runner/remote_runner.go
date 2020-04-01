package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc"
)

type remoteRunner struct {
	runner runner_pb.RunnerClient
}

// NewRemoteRunner creates a Runner that forwards requests to a remote
// gRPC service (typically an instance of bb_runner).
func NewRemoteRunner(connection *grpc.ClientConn) Runner {
	return &remoteRunner{
		runner: runner_pb.NewRunnerClient(connection),
	}
}

func (r *remoteRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	return r.runner.Run(ctx, request)
}
