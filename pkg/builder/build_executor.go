package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/status"
)

func convertErrorToExecuteResponse(err error) *remoteexecution.ExecuteResponse {
	return &remoteexecution.ExecuteResponse{Status: status.Convert(err).Proto()}
}

// BuildExecutor is the interface for the ability to run Bazel execute
// requests and yield an execute response.
type BuildExecutor interface {
	Execute(ctx context.Context, request *remoteexecution.ExecuteRequest) (*remoteexecution.ExecuteResponse, bool)
}
