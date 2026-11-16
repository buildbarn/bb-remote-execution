package builder

import (
	"fmt"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestDescribeSynchronizeRequest(t *testing.T) {
	actionDigest := &remoteexecution.Digest{Hash: "0123abcd", SizeBytes: 42}
	executingRequest := func(update *remoteworker.CurrentState_Executing) *remoteworker.SynchronizeRequest {
		update.ActionDigest = actionDigest
		return &remoteworker.SynchronizeRequest{
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Executing_{Executing: update},
			},
		}
	}
	describe := func(request *remoteworker.SynchronizeRequest, suffix string) string {
		return fmt.Sprintf("request size %d bytes%s", proto.Size(request), suffix)
	}

	t.Run("Unset", func(t *testing.T) {
		request := &remoteworker.SynchronizeRequest{}
		require.Equal(t, describe(request, ", worker state idle"), describeSynchronizeRequest(request))
	})

	t.Run("Idle", func(t *testing.T) {
		request := &remoteworker.SynchronizeRequest{
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}},
			},
		}
		require.Equal(t, describe(request, ", worker state idle"), describeSynchronizeRequest(request))
	})

	t.Run("InProgress", func(t *testing.T) {
		for stateName, update := range map[string]*remoteworker.CurrentState_Executing{
			"started": {
				ExecutionState: &remoteworker.CurrentState_Executing_Started{Started: &emptypb.Empty{}},
			},
			"fetching inputs": {
				ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{FetchingInputs: &emptypb.Empty{}},
			},
			"running": {
				ExecutionState: &remoteworker.CurrentState_Executing_Running{Running: &emptypb.Empty{}},
			},
			"uploading outputs": {
				ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{UploadingOutputs: &emptypb.Empty{}},
			},
		} {
			request := executingRequest(update)
			require.Equal(
				t,
				describe(request, ", action 0123abcd-42, worker state "+stateName),
				describeSynchronizeRequest(request))
		}
	})

	t.Run("Completed", func(t *testing.T) {
		response := &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				OutputFiles:       []*remoteexecution.OutputFile{{Path: "a"}, {Path: "b"}},
				OutputDirectories: []*remoteexecution.OutputDirectory{{Path: "c"}},
				OutputSymlinks:    []*remoteexecution.OutputSymlink{{Path: "d"}, {Path: "e"}, {Path: "f"}},
				StdoutRaw:         []byte("out"),
				StderrRaw:         []byte("error"),
			},
			ServerLogs: map[string]*remoteexecution.LogFile{"log": {}},
		}
		request := executingRequest(&remoteworker.CurrentState_Executing{
			ExecutionState: &remoteworker.CurrentState_Executing_Completed{Completed: response},
		})
		require.Equal(
			t,
			describe(request, fmt.Sprintf(
				", action 0123abcd-42, worker state completed (response size %d bytes, 2 output files, 1 output directories, 3 output symlinks, 3 bytes inline stdout, 5 bytes inline stderr, 1 server logs)",
				proto.Size(response))),
			describeSynchronizeRequest(request))
	})

	t.Run("CompletedWithoutResult", func(t *testing.T) {
		request := executingRequest(&remoteworker.CurrentState_Executing{
			ExecutionState: &remoteworker.CurrentState_Executing_Completed{Completed: &remoteexecution.ExecuteResponse{}},
		})
		require.Equal(
			t,
			describe(request, ", action 0123abcd-42, worker state completed (response size 0 bytes, 0 output files, 0 output directories, 0 output symlinks, 0 bytes inline stdout, 0 bytes inline stderr, 0 server logs)"),
			describeSynchronizeRequest(request))
	})
}

func TestReplaceOversizedResult(t *testing.T) {
	tooLarge := status.Error(codes.ResourceExhausted, "grpc: received message larger than max (6101346 vs. 4194304)")
	completedRequest := func(response *remoteexecution.ExecuteResponse) *remoteworker.SynchronizeRequest {
		return &remoteworker.SynchronizeRequest{
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Executing_{
					Executing: &remoteworker.CurrentState_Executing{
						ActionDigest:   &remoteexecution.Digest{Hash: "0123abcd", SizeBytes: 42},
						ExecutionState: &remoteworker.CurrentState_Executing_Completed{Completed: response},
					},
				},
			},
		}
	}
	result := func() *remoteexecution.ExecuteResponse {
		return &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				OutputFiles: []*remoteexecution.OutputFile{{Path: "a"}},
			},
		}
	}

	t.Run("Replaced", func(t *testing.T) {
		original := result()
		request := completedRequest(original)
		require.True(t, replaceOversizedResult(request, tooLarge))

		replaced := request.CurrentState.GetExecuting().GetCompleted()
		require.Nil(t, replaced.Result)
		require.Equal(t, int32(codes.ResourceExhausted), replaced.Status.Code)
		require.Equal(
			t,
			fmt.Sprintf("Action result of %d bytes is too large to report to the scheduler: grpc: received message larger than max (6101346 vs. 4194304)", proto.Size(original)),
			replaced.Status.Message)
		require.Equal(t, "0123abcd", request.CurrentState.GetExecuting().ActionDigest.Hash)
	})

	t.Run("OtherResourceExhausted", func(t *testing.T) {
		request := completedRequest(result())
		require.False(t, replaceOversizedResult(request, status.Error(codes.ResourceExhausted, "Too many requests")))
		require.NotNil(t, request.CurrentState.GetExecuting().GetCompleted().Result)
	})

	t.Run("OtherCode", func(t *testing.T) {
		request := completedRequest(result())
		require.False(t, replaceOversizedResult(request, status.Error(codes.Unavailable, "larger than max")))
		require.NotNil(t, request.CurrentState.GetExecuting().GetCompleted().Result)
	})

	t.Run("AlreadyReplaced", func(t *testing.T) {
		request := completedRequest(result())
		require.True(t, replaceOversizedResult(request, tooLarge))
		require.False(t, replaceOversizedResult(request, tooLarge))
	})

	t.Run("NotCompleted", func(t *testing.T) {
		idle := &remoteworker.SynchronizeRequest{
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}},
			},
		}
		require.False(t, replaceOversizedResult(idle, tooLarge))
		require.False(t, replaceOversizedResult(&remoteworker.SynchronizeRequest{}, tooLarge))
		running := completedRequest(nil)
		running.CurrentState.GetExecuting().ExecutionState = &remoteworker.CurrentState_Executing_Running{Running: &emptypb.Empty{}}
		require.False(t, replaceOversizedResult(running, tooLarge))
	})
}
