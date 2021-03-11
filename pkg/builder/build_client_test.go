package builder_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildClient(t *testing.T) {
	ctrl := gomock.NewController(t)

	operationQueueClient := mock.NewMockOperationQueueClient(ctrl)
	buildExecutor := mock.NewMockBuildExecutor(ctrl)
	filePool := mock.NewMockFilePool(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	workerID := map[string]string{"hostname": "example.com"}
	instanceName := digest.MustNewInstanceName("main")
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "os", Value: "linux"},
		},
	}
	bc := builder.NewBuildClient(operationQueueClient, buildExecutor, filePool, clock, workerID, instanceName, platform)

	// By default, the client should not be in the executing state.
	require.False(t, bc.InExecutingState())

	// If synchronizing against the scheduler doesn't yield any
	// action to run, the client should remain in the idle state.
	operationQueueClient.EXPECT().Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
		WorkerId:     workerID,
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1010},
	}, nil)
	require.NoError(t, bc.Run())
	require.False(t, bc.InExecutingState())

	// Let the scheduler return an action to execute. This should
	// cause a call against the BuildExecutor.
	desiredStateExecuting1 := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
		Action: &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		},
		QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1007},
	}
	operationQueueClient.EXPECT().Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
		WorkerId:     workerID,
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1020},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: desiredStateExecuting1,
			},
		},
	}, nil)
	buildExecutor.EXPECT().Execute(
		gomock.Any(),
		filePool,
		instanceName,
		desiredStateExecuting1,
		gomock.Any(),
	).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{},
	})
	require.NoError(t, bc.Run())
	require.True(t, bc.InExecutingState())

	// Synchronize against the scheduler to report the successful
	// completion of the action. This should cause the scheduler to
	// immediately return a second action.
	desiredStateExecuting2 := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "8c7bdf20235417b8e3bfa695407e1ff0b43e8223",
			SizeBytes: 123,
		},
		Action: &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "11483c42a98269d01673aa3157836d2882aad5de",
				SizeBytes: 456,
			},
		},
		QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1008},
	}
	clock.EXPECT().Now().Return(time.Unix(1015, 0)).Times(2)
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(5*time.Second).Return(timer1, nil)
	timer1.EXPECT().Stop().Return(true)
	operationQueueClient.EXPECT().Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
		WorkerId:     workerID,
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{},
						},
					},
				},
			},
		},
	}).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1025},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: desiredStateExecuting2,
			},
		},
	}, nil)
	buildExecutor.EXPECT().Execute(
		gomock.Any(),
		filePool,
		instanceName,
		desiredStateExecuting2,
		gomock.Any(),
	).Return(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Failed to contact runner").Proto(),
	})
	require.NoError(t, bc.Run())
	require.True(t, bc.InExecutingState())

	// Also synchronize to report the outcome of the second action.
	// Unlike the first one, it failed catastrophically. This should
	// cause the PreferBeingIdle flag to be set, thereby forcing the
	// scheduler to transition the worker into the idle state. This
	// gives the caller of BuildClient.Run() some time to perform
	// addition health checks before requesting more work.
	clock.EXPECT().Now().Return(time.Unix(1020, 0)).Times(2)
	timer2 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(5*time.Second).Return(timer2, nil)
	timer2.EXPECT().Stop().Return(true)
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:     workerID,
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "8c7bdf20235417b8e3bfa695407e1ff0b43e8223",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Status: status.New(codes.Internal, "Failed to contact runner").Proto(),
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1055},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, nil)
	require.NoError(t, bc.Run())
	require.False(t, bc.InExecutingState())
}
