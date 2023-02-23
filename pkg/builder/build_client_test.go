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
	digestFunction := digest.MustNewFunction("prefix/suffix", remoteexecution.DigestFunction_SHA1)
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "os", Value: "linux"},
		},
	}
	bc := builder.NewBuildClient(operationQueueClient, buildExecutor, filePool, clock, workerID, digest.MustNewInstanceName("prefix"), platform, 4)

	// If synchronizing against the scheduler doesn't yield any
	// action to run, the client should remain in the idle state.
	buildExecutor.EXPECT().CheckReadiness(context.Background())
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1010},
	}, nil)
	mayTerminate, err := bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	// Let the scheduler return an action to execute. This should
	// cause a call against the BuildExecutor.
	buildExecutor.EXPECT().CheckReadiness(context.Background())
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
		QueuedTimestamp:    &timestamppb.Timestamp{Seconds: 1007},
		InstanceNameSuffix: "suffix",
		DigestFunction:     remoteexecution.DigestFunction_SHA1,
	}
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(&remoteworker.SynchronizeResponse{
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
		nil,
		digestFunction,
		desiredStateExecuting1,
		gomock.Any(),
	).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{},
	})
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, false, mayTerminate)
	require.NoError(t, err)

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
		QueuedTimestamp:    &timestamppb.Timestamp{Seconds: 1008},
		InstanceNameSuffix: "suffix",
		DigestFunction:     remoteexecution.DigestFunction_SHA1,
	}
	clock.EXPECT().Now().Return(time.Unix(1015, 0)).Times(2)
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(5*time.Second).Return(timer1, nil)
	timer1.EXPECT().Stop().Return(true)
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
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
	})).Return(&remoteworker.SynchronizeResponse{
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
		nil,
		digestFunction,
		desiredStateExecuting2,
		gomock.Any(),
	).Return(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Failed to contact runner").Proto(),
	})
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, false, mayTerminate)
	require.NoError(t, err)

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
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
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
				},
			},
		},
		PreferBeingIdle: true,
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1055},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, nil)
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	// Because we've transitioned back to idle, the next run can
	// once again do a readiness check. As long as it fails, no
	// synchronizations should be attempted.
	buildExecutor.EXPECT().CheckReadiness(context.Background()).
		Return(status.Error(codes.Internal, "Still cannot contact runner"))
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Worker failed readiness check: Still cannot contact runner"), err)

	// As soon as the readiness check starts to succeed, we should
	// do additional synchronizations.
	buildExecutor.EXPECT().CheckReadiness(context.Background())
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1065},
	}, nil)
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	// If a call to Synchronize() fails, it may be the case we lost
	// an execution request returned by the scheduler. Subsequent
	// iterations should thus not perform readiness checking, as it
	// could cause delays in us trying to pick up the work again.
	buildExecutor.EXPECT().CheckReadiness(context.Background())
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(nil, status.Error(codes.Unavailable, "Connection refused"))
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, false, mayTerminate)
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to synchronize with scheduler: Connection refused"), err)

	for i := 0; i < 10; i++ {
		operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
			WorkerId:           workerID,
			InstanceNamePrefix: "prefix",
			Platform:           platform,
			SizeClass:          4,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
			PreferBeingIdle: true,
		})).Return(nil, status.Error(codes.Unavailable, "Connection refused"))
		mayTerminate, err = bc.Run(context.Background())
		require.Equal(t, false, mayTerminate)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to synchronize with scheduler: Connection refused"), err)
	}

	// Once Synchronize() starts to work once again, we may perform
	// readiness checking again.
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
		PreferBeingIdle: true,
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1070},
	}, nil)
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	buildExecutor.EXPECT().CheckReadiness(context.Background())
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(&remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1071},
	}, nil)
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	// If the build client is in the idle state, attempting to let
	// it run with a canceled context should not do anything, as the
	// worker is capable of terminating immediately.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	mayTerminate, err = bc.Run(canceledCtx)
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)

	// If the build client thinks there is a chance the worker might
	// have handed out an operation, it must attempt to contact the
	// scheduler to mark itself idle, regardless of the context
	// being expired.
	buildExecutor.EXPECT().CheckReadiness(context.Background())
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})).Return(nil, status.Error(codes.Unavailable, "Failed to return response to worker"))
	mayTerminate, err = bc.Run(context.Background())
	require.Equal(t, false, mayTerminate)
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to synchronize with scheduler: Failed to return response to worker"), err)

	clock.EXPECT().Now().Return(time.Unix(1073, 0))
	operationQueueClient.EXPECT().Synchronize(context.Background(), testutil.EqProto(t, &remoteworker.SynchronizeRequest{
		WorkerId:           workerID,
		InstanceNamePrefix: "prefix",
		Platform:           platform,
		SizeClass:          4,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
		PreferBeingIdle: true,
	})).Return(nil, status.Error(codes.Unavailable, "Failed to return response to worker"))
	mayTerminate, err = bc.Run(canceledCtx)
	require.Equal(t, false, mayTerminate)
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to synchronize with scheduler: Failed to return response to worker"), err)

	// Because we don't want termination of the worker to hang if
	// the scheduler is unavailable for a longer amount of time, we
	// only attempt to synchronize for a while. After that we should
	// be permitted to terminate, regardless of whether the
	// scheduler thinks the worker is executing.
	clock.EXPECT().Now().Return(time.Unix(1200, 0))
	mayTerminate, err = bc.Run(canceledCtx)
	require.Equal(t, true, mayTerminate)
	require.NoError(t, err)
}
