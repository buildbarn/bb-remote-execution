package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// NewDefaultExecuteResponse creates an ExecuteResponse message that
// contains all fields that BuildExecutor should set by default.
func NewDefaultExecuteResponse(request *remoteworker.DesiredState_Executing) *remoteexecution.ExecuteResponse {
	return &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: append([]*anypb.Any(nil), request.AuxiliaryMetadata...),
			},
		},
	}
}

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

// getResultAndGRPCCodeFromExecuteResponse converts an ExecuteResponse
// to a pair of strings that describe the execution outcome. These
// strings can be used as part of metrics labels.
func getResultAndGRPCCodeFromExecuteResponse(response *remoteexecution.ExecuteResponse) (result, grpcCode string) {
	if c := status.FromProto(response.Status).Code(); c != codes.OK {
		result = "Failure"
		grpcCode = c.String()
	} else if actionResult := response.Result; actionResult == nil {
		result = "ActionResultMissing"
	} else if actionResult.ExitCode == 0 {
		result = "Success"
	} else {
		result = "NonZeroExitCode"
	}
	return
}

// BuildExecutor is the interface for the ability to run Bazel execute
// requests and yield an execute response.
type BuildExecutor interface {
	Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse
}
