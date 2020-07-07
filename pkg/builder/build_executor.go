package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/status"
)

// attachErrorToExecuteResponse extends an ExecuteResponse to contain an
// error, indicating that the action has failed. If the ExecuteResponse
// already contains an error, it is not overwritten. This is done,
// because the first error is typically the most interesting one to
// return the user. As successive errors may well be related to the
// first, returning all of them would be noisy.
func attachErrorToExecuteResponse(response *remoteexecution.ExecuteResponse, err error) {
	if status.ErrorProto(response.Status) == nil {
		response.Status = status.Convert(err).Proto()
	}
}

// BuildExecutor is the interface for the ability to run Bazel execute
// requests and yield an execute response.
type BuildExecutor interface {
	Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse
}
