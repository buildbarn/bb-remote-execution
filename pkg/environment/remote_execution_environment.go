package environment

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc"
)

type remoteExecutionEnvironment struct {
	buildDirectory filesystem.Directory
	runner         runner.RunnerClient
}

// NewRemoteExecutionEnvironment returns an Environment capable of
// forwarding commands to a GRPC service.
func NewRemoteExecutionEnvironment(client *grpc.ClientConn, buildDirectory filesystem.Directory) Environment {
	return &remoteExecutionEnvironment{
		buildDirectory: buildDirectory,
		runner:         runner.NewRunnerClient(client),
	}
}

func (e *remoteExecutionEnvironment) GetBuildDirectory() filesystem.Directory {
	return e.buildDirectory
}

func (e *remoteExecutionEnvironment) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	return e.runner.Run(ctx, request)
}
