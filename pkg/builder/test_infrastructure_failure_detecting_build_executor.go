package builder

import (
	"context"
	"path"
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/program"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestInfrastructureFailureShutdownState keeps track of whether a group
// of worker threads have shut down, due to an excessive number of
// consecutive tests failing due to infrastructure failures on a single
// worker thread.
type TestInfrastructureFailureShutdownState struct {
	once    sync.Once
	channel chan struct{}
}

// NewTestInfrastructureFailureShutdownState creates a new
// TestInfrastructureFailureShutdownState that is in the initial state,
// where no infrastructure failures have occurred.
func NewTestInfrastructureFailureShutdownState() *TestInfrastructureFailureShutdownState {
	return &TestInfrastructureFailureShutdownState{
		channel: make(chan struct{}),
	}
}

var errTooManyInfrastructureFailures = status.Error(codes.Unavailable, "Worker has shut down, as too many consecutive tests reported an infrastructure failure")

func (ts *TestInfrastructureFailureShutdownState) shutDown() error {
	ts.once.Do(func() {
		close(ts.channel)
	})
	return errTooManyInfrastructureFailures
}

func (ts *TestInfrastructureFailureShutdownState) isShutDown() error {
	select {
	case <-ts.channel:
		return errTooManyInfrastructureFailures
	default:
		return nil
	}
}

func (ts *TestInfrastructureFailureShutdownState) waitForShutdown(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
	select {
	case <-ts.channel:
		return errTooManyInfrastructureFailures
	case <-ctx.Done():
		return nil
	}
}

type testInfrastructureFailureDetectingBuildExecutor struct {
	base                       BuildExecutor
	shutdownState              *TestInfrastructureFailureShutdownState
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
func NewTestInfrastructureFailureDetectingBuildExecutor(base BuildExecutor, shutdownState *TestInfrastructureFailureShutdownState, maximumConsecutiveFailures uint32) BuildExecutor {
	return &testInfrastructureFailureDetectingBuildExecutor{
		base:                       base,
		shutdownState:              shutdownState,
		maximumConsecutiveFailures: maximumConsecutiveFailures,
	}
}

func (be *testInfrastructureFailureDetectingBuildExecutor) CheckReadiness(ctx context.Context) error {
	if err := be.shutdownState.isShutDown(); err != nil {
		return err
	}

	return program.RunLocal(ctx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		dependenciesGroup.Go(be.shutdownState.waitForShutdown)
		return be.base.CheckReadiness(ctx)
	})
}

func (be *testInfrastructureFailureDetectingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	if err := be.shutdownState.isShutDown(); err != nil {
		response := NewDefaultExecuteResponse(request)
		attachErrorToExecuteResponse(response, err)
		return response
	}

	var response *remoteexecution.ExecuteResponse
	if err := program.RunLocal(ctx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		dependenciesGroup.Go(be.shutdownState.waitForShutdown)
		response = be.base.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)

		// Check for the existence of TEST_INFRASTRUCTURE_FAILURE_FILE.
		//
		// TODO: As this BuildExecutor doesn't have access to the
		// Command message, we assume that the file is always called
		// "test.infrastructure_failure". This may not be a valid
		// assumption.
		hasInfrastructureFailure := false
		for _, outputFile := range response.Result.OutputFiles {
			if path.Base(outputFile.Path) == "test.infrastructure_failure" {
				hasInfrastructureFailure = true
				break
			}
		}

		if hasInfrastructureFailure {
			if be.currentConsecutiveFailures.Add(1) >= be.maximumConsecutiveFailures {
				// Too many consecutive test failures.
				// Shut down all worker threads.
				return be.shutdownState.shutDown()
			}
		} else {
			// No infrastructure failure occurred, likely
			// because the test succeeded. Reset the counter
			// for this worker thread.
			be.currentConsecutiveFailures.Store(0)
		}
		return nil
	}); err != nil {
		newResponse := &remoteexecution.ExecuteResponse{}
		proto.Merge(newResponse, response)
		attachErrorToExecuteResponse(newResponse, err)
		return newResponse
	}
	return response
}
