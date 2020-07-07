package builder_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
)

func TestTimestampedBuildExecutorExample(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Recurring messages used by this test.
	actionDigest := &remoteexecution.Digest{
		Hash:      "d41d8cd98f00b204e9800998ecf8427e",
		SizeBytes: 123,
	}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest:    actionDigest,
		QueuedTimestamp: &timestamp.Timestamp{Seconds: 999},
	}
	updateFetchingInputs := &remoteworker.CurrentState_Executing{
		ActionDigest: actionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
			FetchingInputs: &empty.Empty{},
		},
	}
	updateExecuting := &remoteworker.CurrentState_Executing{
		ActionDigest: actionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &empty.Empty{},
		},
	}
	updateUploadingOutputs := &remoteworker.CurrentState_Executing{
		ActionDigest: actionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &empty.Empty{},
		},
	}

	// Simulate the execution of an action where every stage takes
	// one second.
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	filePool := mock.NewMockFilePool(ctrl)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	auxiliaryMetadata, err := ptypes.MarshalAny(&empty.Empty{})
	require.NoError(t, err)
	baseBuildExecutor.EXPECT().Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("main"),
		request,
		gomock.Any()).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		executionStateUpdates <- updateFetchingInputs
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		executionStateUpdates <- updateExecuting
		clock.EXPECT().Now().Return(time.Unix(1003, 0))
		executionStateUpdates <- updateUploadingOutputs
		clock.EXPECT().Now().Return(time.Unix(1004, 0))
		return &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExitCode: 1,
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
					AuxiliaryMetadata: []*any.Any{auxiliaryMetadata},
				},
			},
		}
	})

	// Invoke action through the timestamped build executor.
	executionStateUpdates := make(chan *remoteworker.CurrentState_Executing, 3)
	buildExecutor := builder.NewTimestampedBuildExecutor(baseBuildExecutor, clock, "builder.example.com")
	executeResponse := buildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("main"),
		request,
		executionStateUpdates)

	// Execution updates should be forwarded literally.
	require.Equal(t, <-executionStateUpdates, updateFetchingInputs)
	require.Equal(t, <-executionStateUpdates, updateExecuting)
	require.Equal(t, <-executionStateUpdates, updateUploadingOutputs)

	// Execute response should be augmented to include metadata.
	// Auxiliary metadata that is already part of the execution
	// metadata should not be discarded.
	require.True(t, proto.Equal(executeResponse, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode: 1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				Worker:                         "builder.example.com",
				QueuedTimestamp:                &timestamp.Timestamp{Seconds: 999},
				WorkerStartTimestamp:           &timestamp.Timestamp{Seconds: 1000},
				InputFetchStartTimestamp:       &timestamp.Timestamp{Seconds: 1001},
				InputFetchCompletedTimestamp:   &timestamp.Timestamp{Seconds: 1002},
				ExecutionStartTimestamp:        &timestamp.Timestamp{Seconds: 1002},
				ExecutionCompletedTimestamp:    &timestamp.Timestamp{Seconds: 1003},
				OutputUploadStartTimestamp:     &timestamp.Timestamp{Seconds: 1003},
				OutputUploadCompletedTimestamp: &timestamp.Timestamp{Seconds: 1004},
				WorkerCompletedTimestamp:       &timestamp.Timestamp{Seconds: 1004},
				AuxiliaryMetadata:              []*any.Any{auxiliaryMetadata},
			},
		},
	}))
}
