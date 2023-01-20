package builder

import (
	"context"
	"path"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type testInfrastructureFailureDetectingBuildExecutor struct {
	base                       BuildExecutor
	maximumConsecutiveFailures uint32

	currentConsecutiveFailures atomic.Uint32
}

// NewTestInfrastructureFailureDetectingBuildExecutor is a decorator for
// BuildExecutor that counts the number of consecutive actions that
// generated one or more "test.infrastructure_failure" output files. If
// the count exceeds a configured value, the BuildExecutor will start to
// fail readiness checks. This prevents further work from being
// executed.
//
// This decorator is useful when workers have peripherals attached to
// them that are prone to hardware failures. Bazel allows tests to
// report these failures by creating the file designated by the
// TEST_INFRASTRUCTURE_FAILURE_FILE environment variable.
//
// Please refer to the Bazel test encyclopedia for more details on
// TEST_INFRASTRUCTURE_FAILURE_FILE:
// https://bazel.build/reference/test-encyclopedia
func NewTestInfrastructureFailureDetectingBuildExecutor(base BuildExecutor, maximumConsecutiveFailures uint32) BuildExecutor {
	return &testInfrastructureFailureDetectingBuildExecutor{
		base:                       base,
		maximumConsecutiveFailures: maximumConsecutiveFailures,
	}
}

func (be *testInfrastructureFailureDetectingBuildExecutor) CheckReadiness(ctx context.Context) error {
	if value := be.currentConsecutiveFailures.Load(); value >= be.maximumConsecutiveFailures {
		return status.Errorf(codes.Unavailable, "Worker has shut down, as %d consecutive tests reported an infrastructure failure", value)
	}
	return be.base.CheckReadiness(ctx)
}

func (be *testInfrastructureFailureDetectingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.base.Execute(ctx, filePool, digestFunction, request, executionStateUpdates)

	// Check for the existence of TEST_INFRASTRUCTURE_FAILURE_FILE.
	//
	// TODO: As this BuildExecutor doesn't have access to the
	// Command message, we assume that the file is always called
	// "test.infrastructure_failure". This may not be a valid
	// assumption.
	for _, outputFile := range response.Result.OutputFiles {
		if path.Base(outputFile.Path) == "test.infrastructure_failure" {
			// Action failed with an infrastructure failure.
			if value := be.currentConsecutiveFailures.Add(1); value >= be.maximumConsecutiveFailures {
				newResponse := &remoteexecution.ExecuteResponse{}
				proto.Merge(newResponse, response)
				attachErrorToExecuteResponse(newResponse, status.Errorf(codes.Unavailable, "Worker is shutting down, as %d consecutive tests reported an infrastructure failure", value))
				return newResponse
			}
			return response
		}
	}

	// Action completed without an infrastructure failure.
	be.currentConsecutiveFailures.Store(0)
	return response
}
