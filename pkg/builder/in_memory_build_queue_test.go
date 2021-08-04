package builder_test

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_builder "github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var buildQueueConfigurationForTesting = re_builder.InMemoryBuildQueueConfiguration{
	ExecutionUpdateInterval:              time.Minute,
	OperationWithNoWaitersTimeout:        time.Minute,
	PlatformQueueWithNoWorkersTimeout:    15 * time.Minute,
	BusyWorkerSynchronizationInterval:    10 * time.Second,
	GetIdleWorkerSynchronizationInterval: func() time.Duration { return time.Minute },
	WorkerTaskRetryCount:                 9,
	WorkerWithNoSynchronizationsTimeout:  time.Minute,
}

// getExecutionClient creates a GRPC client for calling Execute() and
// WaitExecution() operations against a build queue. These operations
// use streaming RPCs, which prevents us from invoking these operations
// directly.
//
// By using the bufconn package, we can create a GRPC client and server
// that communicate with each other entirely in memory.
func getExecutionClient(t *testing.T, buildQueue builder.BuildQueue) remoteexecution.ExecutionClient {
	conn := bufconn.Listen(1)
	server := grpc.NewServer()
	remoteexecution.RegisterExecutionServer(server, buildQueue)
	go func() {
		require.NoError(t, server.Serve(conn))
	}()
	client, err := grpc.Dial(
		"myself",
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return conn.Dial()
		}),
		grpc.WithInsecure())
	require.NoError(t, err)
	return remoteexecution.NewExecutionClient(client)
}

func TestInMemoryBuildQueueExecuteBadRequest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// ExecuteRequest contains an invalid action digest.
	t.Run("InvalidActionDigest", func(t *testing.T) {
		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "This is not a valid hash",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, err, status.Error(codes.InvalidArgument, "Failed to extract digest for action: Unknown digest hash length: 24 characters"))
	})

	// Action cannot be found in the Content Addressable Storage (CAS).
	t.Run("MissingAction", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewBufferFromError(status.Error(codes.FailedPrecondition, "Blob not found")))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, err, status.Error(codes.FailedPrecondition, "Failed to obtain action: Blob not found"))
	})

	// Action contains an invalid command digest.
	t.Run("InvalidCommandDigest", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "This is not a valid hash",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to extract digest for command: Unknown digest hash length: 24 characters"), err)
	})

	// Command cannot be found in the Content Addressable Storage (CAS).
	t.Run("MissingCommand", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewBufferFromError(status.Error(codes.FailedPrecondition, "Blob not found")))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, err, status.Error(codes.FailedPrecondition, "Failed to obtain command: Blob not found"))
	})

	// Platform requirements should be provided in sorted order.
	// Otherwise, there could be distinct queues that refer to the
	// same platform.
	t.Run("BadlySortedPlatformProperties", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "os", Value: "linux"},
					{Name: "cpu", Value: "armv6"},
				},
			},
		}, buffer.UserProvided))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, err, status.Error(codes.InvalidArgument, "Platform properties are not sorted"))
	})

	// No workers have registered themselves against this queue,
	// meaninig calls to Execute() should fail unconditionally. A
	// soft error code should be returned if this happens not long
	// after startup, as workers may still appear.
	t.Run("UnknownPlatformSoft", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
		}, buffer.UserProvided))
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassSelector.EXPECT().Abandoned()
		clock.EXPECT().Now().Return(time.Unix(899, 999999999))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "No workers exist for instance name prefix \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"), err)
	})

	// We can be certain that no workers will appear if a sufficient
	// amount of time has passed. We may then start returning a hard
	// error code.
	t.Run("UnknownPlatformHard", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
		}, buffer.UserProvided))
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassSelector.EXPECT().Abandoned()
		clock.EXPECT().Now().Return(time.Unix(900, 0))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "No workers exist for instance name prefix \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"), err)
	})
}

func TestInMemoryBuildQueuePurgeStaleWorkersAndQueues(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	for i := 0; i < 10; i++ {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
			DoNotCache: true,
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{}, buffer.UserProvided))
	}
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// Let a client enqueue a new operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
		DoNotCache: true,
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	initialSizeClassLearner.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Assign it to the worker.
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	// The timer should stop and we should get an immediate
	// update on the state of the operation.
	timer1.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	timer2 := mock.NewMockTimer(ctrl)
	wakeup2 := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer2, wakeup2)

	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						DoNotCache: true,
						Timeout:    &durationpb.Duration{Seconds: 1800},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executingMessage := &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}
	testutil.RequireEqualProto(t, executingMessage, update)

	// The next time the client receives an update on the operation,
	// it should (still) be in the EXECUTING state.
	timer3 := mock.NewMockTimer(ctrl)
	wakeup3 := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer3, wakeup3)
	wakeup2 <- time.Unix(1061, 0)
	update, err = stream1.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, executingMessage, update)

	// Because the worker is not providing any updates, the
	// operation should be terminated.
	// TODO: This could already trigger as soon as 1062, but would
	// require waitExecution() to do a short sleep, which may
	// increase complexity/overhead.
	clock.EXPECT().Now().Return(time.Unix(1121, 0))
	wakeup3 <- time.Unix(1121, 0)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Unavailable, "Worker {\"hostname\":\"worker123\",\"thread\":\"42\"} disappeared while task was executing").Proto(),
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})

	// Even with the worker being gone, it's permitted to enqueue
	// operations for a limited amount of time. These will only be
	// executed if another worker would appear. Spawn eight
	// operations.
	fakeUUIDs := []string{
		"0fb1dd7c-ef72-4a42-94c1-60d7cd587736",
		"1ef11db1-7b06-44ec-b5d2-af3a4b9a249f",
		"59fafbe5-6f5d-4cf9-9ff4-9d320fa11626",
		"62f855dc-9106-44c5-937a-dd33977f92f4",
		"7144c9c0-6684-4bf0-8ada-1b50c52878d0",
		"b331f0b2-b852-476c-95cc-9888aa246a3d",
		"c016e168-2f65-43e8-85d9-7340fc462eb6",
		"eaabd51d-10e7-4b66-a42c-2e00be0daf3d",
	}
	streams := make([]remoteexecution.Execution_ExecuteClient, 0, len(fakeUUIDs))
	for _, fakeUUID := range fakeUUIDs {
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
			DoNotCache: true,
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassLearner := mock.NewMockLearner(ctrl)
		initialSizeClassSelector.EXPECT().Select([]uint32{0}).
			Return(0, 30*time.Minute, initialSizeClassLearner)
		initialSizeClassLearner.EXPECT().Abandoned()
		clock.EXPECT().Now().Return(time.Unix(1961, 999999999))
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		timer.EXPECT().Stop().Return(true)
		uuidGenerator.EXPECT().Call().Return(uuid.Parse(fakeUUID))
		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		update, err = stream.Recv()
		require.NoError(t, err)
		metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &longrunning.Operation{
			Name:     fakeUUID,
			Metadata: metadata,
		}, update)
		streams = append(streams, stream)
	}

	// After workers are absent for long enough, the corresponding
	// platform queue is also garbage collected.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector = mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
		DoNotCache: true,
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassSelector.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1962, 0)).Times(17)
	stream3, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	_, err = stream3.Recv()
	testutil.RequireEqualStatus(t, err, status.Error(codes.FailedPrecondition, "No workers exist for instance name prefix \"main\" platform {}"))

	// Operations that were queued should have been cancelled when
	// the platform queue was garbage collected. All eight should
	// get woken up.
	for i, fakeUUID := range fakeUUIDs {
		update, err = streams[i].Recv()
		require.NoError(t, err)
		metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_COMPLETED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		executeResponse, err = anypb.New(&remoteexecution.ExecuteResponse{
			Status: status.New(codes.Unavailable, "Workers for this instance name, platform and size class disappeared while task was queued").Proto(),
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     fakeUUID,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: executeResponse},
		})
	}
}

func TestInMemoryBuildQueuePurgeStaleOperations(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	for i := 0; i < 2; i++ {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
		}, buffer.UserProvided))
	}
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), testutil.EqProto(t, action), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector1 := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector1, nil)
	initialSizeClassLearner1 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector1.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner1)
	clock.EXPECT().Now().Return(time.Unix(1070, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	timer1.EXPECT().Stop().Return(true)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	ctx1, cancel1 := context.WithCancel(ctx)
	stream1, err := executionClient.Execute(ctx1, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let a second client enqueue the same action. Due to
	// deduplication of in-flight actions, it will obtain the same
	// operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector2 := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector2, nil)
	initialSizeClassSelector2.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1075, 0))
	timer2 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer2, nil)
	timer2.EXPECT().Stop().Return(true)
	ctx2, cancel2 := context.WithCancel(ctx)
	stream2, err := executionClient.Execute(ctx2, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err = stream2.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let a third client use WaitExecution() to block on the same
	// operation.
	clock.EXPECT().Now().Return(time.Unix(1080, 0)).Times(2)
	timer3 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer3, nil)
	timer3.EXPECT().Stop().Return(true)
	ctx3, cancel3 := context.WithCancel(ctx)
	stream3, err := executionClient.WaitExecution(ctx3, &remoteexecution.WaitExecutionRequest{
		Name: "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
	})
	require.NoError(t, err)
	update, err = stream3.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// The operation should be present without any timeout
	// associated with it, as there are multiple waiters.
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "main",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1080, 0))
	allOperations, err := buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{
		PageSize: 10,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListOperationsResponse{
		Operations: []*buildqueuestate.OperationState{
			{
				Name:               "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
				SizeClassQueueName: sizeClassQueueName,
				InvocationId:       &anypb.Any{},
				QueuedTimestamp:    &timestamppb.Timestamp{Seconds: 1070},
				ActionDigest: &remoteexecution.Digest{
					Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
					SizeBytes: 123,
				},
				Stage: &buildqueuestate.OperationState_Queued{
					Queued: &emptypb.Empty{},
				},
			},
		},
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 1,
		},
	}, allOperations)

	// Cancel all Execute() and WaitExecution() calls.
	cancelWait := make(chan struct{})
	clock.EXPECT().Now().Return(time.Unix(1090, 0)).Times(3).Do(func() {
		cancelWait <- struct{}{}
	})
	cancel1()
	<-cancelWait
	cancel2()
	<-cancelWait
	cancel3()
	<-cancelWait

	// The operation should still be available up until the deadline.
	clock.EXPECT().Now().Return(time.Unix(1149, 999999999))
	allOperations, err = buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{
		PageSize: 10,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListOperationsResponse{
		Operations: []*buildqueuestate.OperationState{
			{
				Name:               "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
				SizeClassQueueName: sizeClassQueueName,
				InvocationId:       &anypb.Any{},
				QueuedTimestamp:    &timestamppb.Timestamp{Seconds: 1070},
				ActionDigest: &remoteexecution.Digest{
					Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
					SizeBytes: 123,
				},
				Timeout: &timestamppb.Timestamp{Seconds: 1150},
				Stage: &buildqueuestate.OperationState_Queued{
					Queued: &emptypb.Empty{},
				},
			},
		},
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 1,
		},
	}, allOperations)

	// And it should be gone after it.
	initialSizeClassLearner1.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1150, 0))
	allOperations, err = buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{
		PageSize: 10,
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(&buildqueuestate.ListOperationsResponse{
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 0,
		},
	}, allOperations))
}

func TestInMemoryBuildQueueKillOperation(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main/suffix", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main/suffix", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
	}, buffer.UserProvided))
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main/suffix"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	timer := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main/suffix",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let the same worker repeatedly ask for work. It should
	// constantly get the same operation assigned. This may happen
	// when the network is flaky or the worker is crash-looping.

	// At the first iteration, the worker will report execution start.
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	timer = mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)

	for i := int64(0); i < 10; i++ {
		clock.EXPECT().Now().Return(time.Unix(1002+i, 0))
		response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			InstanceNamePrefix: "main",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012 + i},
			DesiredState: &remoteworker.DesiredState{
				WorkerState: &remoteworker.DesiredState_Executing_{
					Executing: &remoteworker.DesiredState_Executing{
						ActionDigest: &remoteexecution.Digest{
							Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
							SizeBytes: 123,
						},
						Action: &remoteexecution.Action{
							CommandDigest: &remoteexecution.Digest{
								Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
								SizeBytes: 456,
							},
							Timeout: &durationpb.Duration{Seconds: 1800},
						},
						QueuedTimestamp:    &timestamppb.Timestamp{Seconds: 1001},
						InstanceNameSuffix: "suffix",
					},
				},
			},
		}, response)

		// At the first iteration, we expect an execution-start message.
		if i == 0 {
			update, err = stream1.Recv()
			require.NoError(t, err)
			metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
				Stage: remoteexecution.ExecutionStage_EXECUTING,
				ActionDigest: &remoteexecution.Digest{
					Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
					SizeBytes: 123,
				},
			})
			testutil.RequireEqualProto(t, &longrunning.Operation{
				Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
				Metadata: metadata,
			}, update)
		}
	}

	// Requesting the same operation too many times should cause the
	// scheduler to give up on handing out the same operation. We
	// don't want a single operation to crash-loop a worker
	// indefinitely.
	initialSizeClassLearner.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1012, 0)).Times(3)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// The client should be informed that the operation causes the
	// worker to crash-loop.
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Attempted to execute task 10 times, but it never completed. This task may cause worker {\"hostname\":\"worker123\",\"thread\":\"42\"} to crash.").Proto(),
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})
}

func TestInMemoryBuildQueueCrashLoopingWorker(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
	}, buffer.UserProvided))
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	timer := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let the worker extract the operation from the queue.
	clock.EXPECT().Now().Return(time.Unix(1002, 0)).Times(2)
	timer = mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 1800},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response)
	// The client should be notified the the operation has started executing.
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}, update)

	// Kill the operation.
	initialSizeClassLearner.EXPECT().Abandoned()
	clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(3)
	_, err = buildQueue.KillOperation(ctx, &buildqueuestate.KillOperationRequest{
		OperationName: "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
	})
	require.NoError(t, err)

	// The client should be informed that the operation was killed.
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Unavailable, "Operation was killed administratively").Proto(),
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})

	// The worker should be requested to switch back to idle the
	// next time it contacts the scheduler.
	clock.EXPECT().Now().Return(time.Unix(1012, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
}

func TestInMemoryBuildQueueIdleWorkerSynchronizationTimeout(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())

	// When no work appears, workers should still be woken up
	// periodically to resynchronize. This ensures that workers that
	// disappear without closing their TCP connections are purged
	// quickly.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	timer := mock.NewMockTimer(ctrl)
	timerChannel := make(chan time.Time, 1)
	timerChannel <- time.Unix(1060, 0)
	timer.EXPECT().Stop()
	clock.EXPECT().NewTimer(time.Minute).Return(timer, timerChannel)
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1060},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
}

func TestInMemoryBuildQueueDrainedWorker(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
	}, buffer.UserProvided))
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// The worker should not be drained by default.
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "main",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	workerState, err := buildQueue.ListWorkers(ctx, &buildqueuestate.ListWorkersRequest{
		SizeClassQueueName: sizeClassQueueName,
		PageSize:           1000,
	})
	require.NoError(t, err)
	require.Equal(t, &buildqueuestate.ListWorkersResponse{
		Workers: []*buildqueuestate.WorkerState{
			{
				Id: map[string]string{
					"hostname": "worker123",
					"thread":   "42",
				},
				Timeout: &timestamppb.Timestamp{Seconds: 1060},
				Drained: false,
			},
		},
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 1,
		},
	}, workerState)

	// Adding a drain that doesn't match the worker should cause no
	// changes.
	clock.EXPECT().Now().Return(time.Unix(1003, 0))
	_, err = buildQueue.AddDrain(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		SizeClassQueueName: sizeClassQueueName,
		WorkerIdPattern: map[string]string{
			"hostname": "worker124",
		},
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	workerState, err = buildQueue.ListWorkers(ctx, &buildqueuestate.ListWorkersRequest{
		SizeClassQueueName: sizeClassQueueName,
		PageSize:           1000,
	})
	require.NoError(t, err)
	require.Equal(t, &buildqueuestate.ListWorkersResponse{
		Workers: []*buildqueuestate.WorkerState{
			{
				Id: map[string]string{
					"hostname": "worker123",
					"thread":   "42",
				},
				Timeout: &timestamppb.Timestamp{Seconds: 1060},
				Drained: false,
			},
		},
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 1,
		},
	}, workerState)

	// Adding a drain that does match the worker should cause it to
	// be reported as if being drained.
	clock.EXPECT().Now().Return(time.Unix(1005, 0))
	_, err = buildQueue.AddDrain(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		SizeClassQueueName: sizeClassQueueName,
		WorkerIdPattern: map[string]string{
			"hostname": "worker123",
		},
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	workerState, err = buildQueue.ListWorkers(ctx, &buildqueuestate.ListWorkersRequest{
		SizeClassQueueName: sizeClassQueueName,
		PageSize:           1000,
	})
	require.NoError(t, err)
	require.Equal(t, &buildqueuestate.ListWorkersResponse{
		Workers: []*buildqueuestate.WorkerState{
			{
				Id: map[string]string{
					"hostname": "worker123",
					"thread":   "42",
				},
				Timeout: &timestamppb.Timestamp{Seconds: 1060},
				Drained: true,
			},
		},
		PaginationInfo: &buildqueuestate.PaginationInfo{
			StartIndex:   0,
			TotalEntries: 1,
		},
	}, workerState)

	// Enqueue an operation.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Because the worker is drained, the scheduler should not be
	// willing to return the operation.
	clock.EXPECT().Now().Return(time.Unix(1008, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1008},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	// Remove the drain. The scheduler should now return the
	// operation if requested.
	clock.EXPECT().Now().Return(time.Unix(1009, 0))
	_, err = buildQueue.RemoveDrain(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		SizeClassQueueName: sizeClassQueueName,
		WorkerIdPattern: map[string]string{
			"hostname": "worker123",
		},
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1010, 0)).Times(2)
	timer1.EXPECT().Stop().Return(true)
	clock.EXPECT().NewTimer(time.Minute).Return(nil, nil)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1020},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 1800},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1007},
				},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}, update)
}

func TestInMemoryBuildQueueInvocationFairness(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})

	operationParameters := [...]struct {
		invocationID  string
		actionHash    string
		commandHash   string
		operationName string
	}{
		{"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac", "f4d362da1f854e54984275aa78e2d9f8", "5fbd808d5cf24a219824664040a95c44", "93e2738a-837c-4524-9f0f-430b47caa889"},
		{"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac", "25b997dcfbe34f95bbbab10bc02e1a61", "2ac9ddc1a64442fabd819358a206909e", "fadbaf2f-669f-47ee-bce4-35f255d2ba16"},
		{"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac", "deb57a22d6b149b2b83d26dbc30d8f28", "5def73fca7c945d3998d984194879f5f", "1d320c02-399a-40bb-bb9a-031960c7e542"},
		{"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac", "13590054c8aa457d88a2abc8a0a76d32", "6f9f4cc9c831495bbee40d5a4c9dd183", "a5fb4aeb-e444-4851-b207-7c4da7407955"},
		{"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac", "697a50f669144463acb45cb23a195aee", "756a36b01ff148c88a43fcc240accb2b", "7359bd3c-32b0-40b9-9d8d-341420267d8e"},
		{"4712de52-4518-4840-91d5-c9e13a38cc5a", "d35515d9ebe349639957da438a21bd3c", "9f58a3e3feb044e2a31ffad73479185b", "e7922f00-fc9b-4390-97fe-8564a00d190a"},
		{"4712de52-4518-4840-91d5-c9e13a38cc5a", "f8362f9b02b04765b4873afce32aa7df", "6196409166614f5691f34b18affaa3a8", "76f7be3b-a685-4ac0-b770-d4305aabab5f"},
		{"4712de52-4518-4840-91d5-c9e13a38cc5a", "1ee144ab7e7c43cfaa46ca1ccecad26e", "391d52750e9c494cac10dc72e8f7e3db", "7d206d16-9e83-4f91-a515-0ee8132026ee"},
		{"4712de52-4518-4840-91d5-c9e13a38cc5a", "15f5459fc4004ab0b95f51a9b933648a", "352f5536cd304f24947019801b357ec1", "28ae0c5a-3b93-4ac0-b434-b2d104b43562"},
		{"4712de52-4518-4840-91d5-c9e13a38cc5a", "916792a1220a4d81b6135adbae25c75d", "500673772b944b9f8ee654ea0adb2961", "bfeedf7c-29ec-437a-8b61-dedb2a73a3e4"},
		{"351fdffe-04df-4fe0-98c7-b4f5a463fd52", "9a5d807902e048e896a0d7c437943598", "57efd9f34d664874a4bbba85e90a1515", "91a7f281-b9af-4f06-85f3-751a01baf7ba"},
		{"351fdffe-04df-4fe0-98c7-b4f5a463fd52", "449a88c7885f4dbb94383e496646cfd2", "64fb977929214df794b30ad1d896c212", "081cffbb-4e14-44ff-b4a0-bd7c1ceadd6c"},
		{"351fdffe-04df-4fe0-98c7-b4f5a463fd52", "6fcd0a2f3c0040f2902891d4557cddc0", "c3ad5148dd304a74b010d948f606784e", "489af73a-2c3a-429b-9fa0-22129fbddfd2"},
		{"351fdffe-04df-4fe0-98c7-b4f5a463fd52", "219cc379cdcc4e4fa2cae57b251c5582", "d95707fc59364e4593947262d2d64cf9", "61fdcf8f-5212-45eb-8e72-30b913e8cce9"},
		{"351fdffe-04df-4fe0-98c7-b4f5a463fd52", "97e481163453429a88cfdad7e8ce96f8", "1f12e1fb0ee9430b9a1b172b96dad01a", "a1384f92-1737-4d60-8714-f6e517fe4f5d"},
		{"4fd2080e-d805-4d68-ab51-460106c8f372", "82fdcadeca184486a3c6a3aa780c6fe9", "493ea806febb4cd1803e6ad9ea58746b", "13787980-1f0c-4ff3-912a-866d86039696"},
		{"4fd2080e-d805-4d68-ab51-460106c8f372", "d1384d0de35c4bab9f424a8be6283030", "9e14e453d9ef4abb9c6fc98edd248641", "8e20ef3a-0916-40b5-a733-8256257e05c8"},
		{"4fd2080e-d805-4d68-ab51-460106c8f372", "eff21bbd08904e8188e2946285ff0de3", "c170201058794b9bb97b20e8258080cb", "1c26e52f-fa8b-4d73-bba6-cd423a099244"},
		{"4fd2080e-d805-4d68-ab51-460106c8f372", "08183d1fcb694554b092cd629b5b9b47", "ef2de9a7bf5d4ed2bb08ead541c9f36c", "8f773c13-3d96-4024-b0a0-ca9502818366"},
		{"4fd2080e-d805-4d68-ab51-460106c8f372", "f81d21e375fc4dc6ad5b74fe1f966ecf", "99d95d845f2e4aadbeab1c7b6192d1c8", "15d2c9be-220b-4a58-9052-9a19b58e571a"},
		{"66275a66-8aad-498a-9b4a-26b4f5a66789", "69df79b84df94001bd261101f4e3b092", "ca42e214f92a4d6fab0d697b5f7f539a", "299bde44-6432-4ef6-a3fd-8349ada25a14"},
		{"66275a66-8aad-498a-9b4a-26b4f5a66789", "51a015f6ab8f495a9bcba9569932b8d4", "1f8d98a889234c8288c94c333744b2a7", "e67f2cfa-88b0-4d2c-88d0-a4b025eb63d5"},
		{"66275a66-8aad-498a-9b4a-26b4f5a66789", "94713786eca2417aa18f499a9d72a29b", "168696de2a0b408c933d316a55e52500", "63b3cf66-1401-4ae6-9177-aa2c7a1a2b7a"},
		{"66275a66-8aad-498a-9b4a-26b4f5a66789", "88ed817ebd1340aab4711750196ed8b1", "087b8173438348be9ed2cd6e7a04d49f", "f158a947-a4ed-4c5c-9a6e-053f73b4039f"},
		{"66275a66-8aad-498a-9b4a-26b4f5a66789", "e74171e0a6934f65b2895624a66680fc", "22af639fb2714f5f8b9beedb1fb519a6", "1a843f0e-f234-40c0-86c3-0dec2b2b9f21"},
	}

	// Let 5 clients (based on distinct invocation IDs) enqueue a
	// total of 25 operations for different actions. No in-flight
	// deduplication should take place.
	streams := make([]remoteexecution.Execution_ExecuteClient, 0, 25)
	for i, p := range operationParameters {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", p.actionHash, 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      p.commandHash,
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", p.commandHash, 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: platform,
		}, buffer.UserProvided))

		requestMetadata := &remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		}
		requestMetadataAny, err := anypb.New(requestMetadata)
		require.NoError(t, err)
		requestMetadataBin, err := proto.Marshal(requestMetadata)
		require.NoError(t, err)
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), testutil.EqProto(t, requestMetadata)).Return(requestMetadataAny, nil)

		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      p.commandHash,
				SizeBytes: 456,
			},
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassLearner := mock.NewMockLearner(ctrl)
		initialSizeClassSelector.EXPECT().Select([]uint32{0}).
			Return(0, 30*time.Minute, initialSizeClassLearner)
		initialSizeClassLearner.EXPECT().Abandoned()

		clock.EXPECT().Now().Return(time.Unix(1010+int64(i), 0))
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		timer.EXPECT().Stop().Return(true)
		uuidGenerator.EXPECT().Call().Return(uuid.Parse(p.operationName))
		stream, err := executionClient.Execute(
			metadata.AppendToOutgoingContext(
				ctx,
				"build.bazel.remote.execution.v2.requestmetadata-bin",
				string(requestMetadataBin)),
			&remoteexecution.ExecuteRequest{
				InstanceName: "main",
				ActionDigest: &remoteexecution.Digest{
					Hash:      p.actionHash,
					SizeBytes: 123,
				},
			})
		require.NoError(t, err)
		streams = append(streams, stream)
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      p.actionHash,
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
		})
	}

	// Check that ListInvocations() reports all five invocations,
	// both when justQueuedInvocations is true and false. When true,
	// the invocations should be returned in scheduling order.
	// Otherwise, they should be returned alphabetically.
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "main",
			Platform:           platform,
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1036, 0))
	invocationStates, err := buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
	})
	require.NoError(t, err)
	require.Len(t, invocationStates.Invocations, 5)
	for i, toolInvocationID := range []string{
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
	} {
		invocationID, err := anypb.New(&remoteexecution.RequestMetadata{
			ToolInvocationId: toolInvocationID,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, invocationID, invocationStates.Invocations[i].Id)
		require.Equal(t, uint32(5), invocationStates.Invocations[i].QueuedOperationsCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].ExecutingWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleSynchronizingWorkersCount)
	}

	clock.EXPECT().Now().Return(time.Unix(1036, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
	})
	require.NoError(t, err)
	require.Len(t, invocationStates.Invocations, 5)
	for i, toolInvocationID := range []string{
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
	} {
		invocationID, err := anypb.New(&remoteexecution.RequestMetadata{
			ToolInvocationId: toolInvocationID,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, invocationID, invocationStates.Invocations[i].Id)
		require.Equal(t, uint32(5), invocationStates.Invocations[i].QueuedOperationsCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].ExecutingWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleSynchronizingWorkersCount)
	}

	// Let 25 workers execute the operations that were created
	// previously. Because the operations originated from different
	// client invocations, we should not execute them in the order
	// in which they arrived. Instead, we should alternate between
	// invocations, so that each of them gets their fair share.
	for _, i := range []int{
		0, 5, 10, 15, 20,
		1, 6, 11, 16, 21,
		2, 7, 12, 17, 22,
		3, 8, 13, 18, 23,
		4, 9, 14, 19, 24,
	} {
		p := operationParameters[i]
		clock.EXPECT().Now().Return(time.Unix(1040, 0)).Times(2)
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		timer.EXPECT().Stop().Return(true)
		response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   strconv.FormatInt(int64(i), 10),
			},
			InstanceNamePrefix: "main",
			Platform:           platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		})
		require.NoError(t, err)
		requestMetadata, err := anypb.New(&remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1050},
			DesiredState: &remoteworker.DesiredState{
				WorkerState: &remoteworker.DesiredState_Executing_{
					Executing: &remoteworker.DesiredState_Executing{
						ActionDigest: &remoteexecution.Digest{
							Hash:      p.actionHash,
							SizeBytes: 123,
						},
						Action: &remoteexecution.Action{
							CommandDigest: &remoteexecution.Digest{
								Hash:      p.commandHash,
								SizeBytes: 456,
							},
							Timeout: &durationpb.Duration{Seconds: 1800},
						},
						QueuedTimestamp:   &timestamppb.Timestamp{Seconds: 1010 + int64(i)},
						AuxiliaryMetadata: []*anypb.Any{requestMetadata},
					},
				},
			},
		}, response)

		update, err := streams[i].Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_EXECUTING,
			ActionDigest: &remoteexecution.Digest{
				Hash:      p.actionHash,
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
		}, update)
	}

	// Call ListInvocations() again. All operations should now be
	// reported as executing, instead of being queued.
	clock.EXPECT().Now().Return(time.Unix(1041, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
	})
	require.NoError(t, err)
	require.Empty(t, invocationStates.Invocations)

	clock.EXPECT().Now().Return(time.Unix(1042, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
	})
	require.NoError(t, err)
	require.Len(t, invocationStates.Invocations, 5)
	for i, toolInvocationID := range []string{
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
	} {
		invocationID, err := anypb.New(&remoteexecution.RequestMetadata{
			ToolInvocationId: toolInvocationID,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, invocationID, invocationStates.Invocations[i].Id)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].QueuedOperationsCount)
		require.Equal(t, uint32(5), invocationStates.Invocations[i].ExecutingWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleWorkersCount)
		require.Equal(t, uint32(0), invocationStates.Invocations[i].IdleSynchronizingWorkersCount)
	}

	clock.EXPECT().Now().Return(time.Unix(1043, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ALL,
	})
	require.NoError(t, err)
	require.Len(t, invocationStates.Invocations, 5)

	// Call ListInvocations() a final time after letting a
	// sufficient amount of time pass. This should cause all workers
	// to be removed from the scheduler, as they didn't provide any
	// updates. All associated operations should be completed,
	// meaning that no invocations will be reported.
	clock.EXPECT().Now().Return(time.Unix(1200, 0)).Times(51)
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
	})
	require.NoError(t, err)
	require.Empty(t, invocationStates.Invocations)

	clock.EXPECT().Now().Return(time.Unix(1200, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
	})
	require.NoError(t, err)
	require.Empty(t, invocationStates.Invocations)

	clock.EXPECT().Now().Return(time.Unix(1200, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ALL,
	})
	require.NoError(t, err)
	require.Empty(t, invocationStates.Invocations)

	// All clients should receive an error that their operations
	// terminated due to the loss of workers.
	for i, p := range operationParameters {
		update, err := streams[i].Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_COMPLETED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      p.actionHash,
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
			Status: status.Newf(codes.Unavailable, "Worker {\"hostname\":\"worker123\",\"thread\":\"%d\"} disappeared while task was executing", i).Proto(),
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: executeResponse},
		})

		_, err = streams[i].Recv()
		require.Equal(t, io.EOF, err)
	}
}

// Test what happens when multiple operations are in-flight deduplicated
// against the same underlying task, and are subsequently abandoned
// while being in the QUEUED stage. This should cause all associated
// operations and invocations to be removed eventually.
func TestInMemoryBuildQueueInFlightDeduplicationAbandonQueued(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// Let ten clients create ten operations. Because they all refer
	// to the same action, all requests should be deduplicated into
	// the same task. This means that we create ten initial size
	// class selectors, of which the last nine are abandoned
	// immediately.
	operationParameters := [...]struct {
		invocationID  string
		operationName string
	}{
		{"0f0f22ec-908a-4ea7-8a78-b92ab4188e78", "b4667823-9f8e-451d-a3e4-4481ec67329f"},
		{"0f67bd82-2867-45ec-9412-f058f27d2686", "1b9e4aaf-b984-4ebc-9b51-0e31bf1b0edb"},
		{"3e3975fa-d723-42c6-bccb-a3358793f656", "e662fb47-f162-41b8-b29c-45b24fe9e273"},
		{"557cd041-1d24-423c-9733-f94c8d2916b2", "def137ac-7724-43ff-98f9-b16a3ba01dcd"},
		{"56a827ff-d0bb-4f90-839d-eb55d8060269", "64943e71-86c3-4153-a760-76c0ff30cd68"},
		{"849810af-2e0b-45ae-965d-28642d6c6453", "da009be0-93fe-40ad-9e03-a14e2bee2ff9"},
		{"9cadf0eb-1e28-49ea-b052-5d05cdc50303", "e0f4e177-369d-4412-a19c-b7b1969dd46e"},
		{"9ff4fd36-7123-4b59-90e2-7f49cd0af05e", "34f633ac-c418-4a1d-8a69-796990008e9c"},
		{"d0438436-cff3-45e1-9c0b-7e5af632c0a4", "46cdaa7c-6bfa-49e2-822e-31be760c51c5"},
		{"e4896008-d596-44c7-8df6-6ced53dff6b0", "88929b3e-f664-4f11-873d-40324d06378e"},
	}

	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	for i := 0; i < len(operationParameters)-1; i++ {
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
				SizeBytes: 456,
			},
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassSelector.EXPECT().Abandoned()
	}

	for i, p := range operationParameters {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
				SizeBytes: 456,
			},
		}, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: platform,
		}, buffer.UserProvided))

		requestMetadata := &remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		}
		requestMetadataAny, err := anypb.New(requestMetadata)
		require.NoError(t, err)
		requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		})
		require.NoError(t, err)
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), testutil.EqProto(t, requestMetadata)).Return(requestMetadataAny, nil)

		ctxWithCancel, cancel := context.WithCancel(ctx)

		clock.EXPECT().Now().Return(time.Unix(1010+int64(i), 0))
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		timer.EXPECT().Stop().Return(true)
		uuidGenerator.EXPECT().Call().Return(uuid.Parse(p.operationName))
		stream, err := executionClient.Execute(
			metadata.AppendToOutgoingContext(
				ctxWithCancel,
				"build.bazel.remote.execution.v2.requestmetadata-bin",
				string(requestMetadataBin)),
			&remoteexecution.ExecuteRequest{
				InstanceName: "main",
				ActionDigest: &remoteexecution.Digest{
					Hash:      "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2",
					SizeBytes: 123,
				},
			})
		require.NoError(t, err)
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
		}, update)

		// Immediately cancel the request. The operation should
		// still be valid for the next minute.
		//
		// Because cancelling the RPC happens asynchronously, we
		// wait on clock.Now() to be called to ensure
		// InMemoryBuildQueue has detected the cancelation.
		cancelWait := make(chan struct{})
		clock.EXPECT().Now().Return(time.Unix(1010+int64(i), 0)).Do(func() {
			cancelWait <- struct{}{}
		})
		cancel()
		<-cancelWait
	}

	// Get listings of invocations known by the scheduler. Because
	// we're requesting this one minute after the operations were
	// created, we should gradually see this list shrink. Eventually
	// all invocations should be removed.
	initialSizeClassLearner.EXPECT().Abandoned()
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "main",
			Platform:           platform,
		},
	}
	for i := 0; i <= len(operationParameters); i++ {
		clock.EXPECT().Now().Return(time.Unix(1069+int64(i), 0)).Times(3)

		invocationStates, err := buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_ALL,
		})
		require.NoError(t, err)
		require.Len(t, invocationStates.Invocations, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
		})
		require.NoError(t, err)
		require.Len(t, invocationStates.Invocations, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
		})
		require.NoError(t, err)
		require.Len(t, invocationStates.Invocations, len(operationParameters)-i)
	}
}

// This test is identical to the previous one, except that the operation
// is placed in the EXECUTING stage when being abandoned. The logic for
// removing such operations is different from operations in the QUEUED
// stage.
func TestInMemoryBuildQueueInFlightDeduplicationAbandonExecuting(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// Let ten clients create ten operations. Because they all refer
	// to the same action, all requests should be deduplicated into
	// the same task. This means that we create ten initial size
	// class selectors, of which the last nine are abandoned
	// immediately.
	operationParameters := [...]struct {
		invocationID  string
		operationName string
	}{
		{"0f0f22ec-908a-4ea7-8a78-b92ab4188e78", "b4667823-9f8e-451d-a3e4-4481ec67329f"},
		{"0f67bd82-2867-45ec-9412-f058f27d2686", "1b9e4aaf-b984-4ebc-9b51-0e31bf1b0edb"},
		{"3e3975fa-d723-42c6-bccb-a3358793f656", "e662fb47-f162-41b8-b29c-45b24fe9e273"},
		{"557cd041-1d24-423c-9733-f94c8d2916b2", "def137ac-7724-43ff-98f9-b16a3ba01dcd"},
		{"56a827ff-d0bb-4f90-839d-eb55d8060269", "64943e71-86c3-4153-a760-76c0ff30cd68"},
		{"849810af-2e0b-45ae-965d-28642d6c6453", "da009be0-93fe-40ad-9e03-a14e2bee2ff9"},
		{"9cadf0eb-1e28-49ea-b052-5d05cdc50303", "e0f4e177-369d-4412-a19c-b7b1969dd46e"},
		{"9ff4fd36-7123-4b59-90e2-7f49cd0af05e", "34f633ac-c418-4a1d-8a69-796990008e9c"},
		{"d0438436-cff3-45e1-9c0b-7e5af632c0a4", "46cdaa7c-6bfa-49e2-822e-31be760c51c5"},
		{"e4896008-d596-44c7-8df6-6ced53dff6b0", "88929b3e-f664-4f11-873d-40324d06378e"},
	}

	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
			SizeBytes: 456,
		},
		Platform: platform,
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	for i := 0; i < len(operationParameters)-1; i++ {
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
				SizeBytes: 456,
			},
			Platform: platform,
		})).Return(initialSizeClassSelector, nil)
		initialSizeClassSelector.EXPECT().Abandoned()
	}

	for i, p := range operationParameters {
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
				SizeBytes: 456,
			},
			Platform: platform,
		}, buffer.UserProvided))

		requestMetadata := &remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
			TargetId:         "//:hello_world",
		}
		requestMetadataAny, err := anypb.New(requestMetadata)
		require.NoError(t, err)
		requestMetadataBin, err := proto.Marshal(requestMetadata)
		require.NoError(t, err)
		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), testutil.EqProto(t, requestMetadata)).Return(requestMetadataAny, nil)

		ctxWithCancel, cancel := context.WithCancel(ctx)

		clock.EXPECT().Now().Return(time.Unix(1010+int64(i), 0))
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		timer.EXPECT().Stop().Return(true)
		uuidGenerator.EXPECT().Call().Return(uuid.Parse(p.operationName))
		stream, err := executionClient.Execute(
			metadata.AppendToOutgoingContext(
				ctxWithCancel,
				"build.bazel.remote.execution.v2.requestmetadata-bin",
				string(requestMetadataBin)),
			&remoteexecution.ExecuteRequest{
				InstanceName: "main",
				ActionDigest: &remoteexecution.Digest{
					Hash:      "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2",
					SizeBytes: 123,
				},
			})
		require.NoError(t, err)
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
		}, update)

		// Immediately cancel the request. The operation should
		// still be valid for the next minute.
		//
		// Because cancelling the RPC happens asynchronously, we
		// wait on clock.Now() to be called to ensure
		// InMemoryBuildQueue has detected the cancelation.
		cancelWait := make(chan struct{})
		clock.EXPECT().Now().Return(time.Unix(1010+int64(i), 0)).Do(func() {
			cancelWait <- struct{}{}
		})
		cancel()
		<-cancelWait
	}

	// Let one worker execute the task. Because of in-flight
	// deduplication, all ten operations should now be in the
	// EXECUTING stage.
	clock.EXPECT().Now().Return(time.Unix(1065, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	requestMetadata, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: "0f0f22ec-908a-4ea7-8a78-b92ab4188e78",
		TargetId:         "//:hello_world",
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1075},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "fc96ea0eee854b45950d3a7448332445730886691b992cb7917da0853664f7c2",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "f7a3ac7c17e535bc9b54ab13dbbb95a52ca1f1edaf9503ce23ccb3eca331a4f5",
							SizeBytes: 456,
						},
						Platform: platform,
						Timeout:  &durationpb.Duration{Seconds: 1800},
					},
					QueuedTimestamp:   &timestamppb.Timestamp{Seconds: 1010},
					AuxiliaryMetadata: []*anypb.Any{requestMetadata},
				},
			},
		},
	}, response)

	// Get listings of invocations known by the scheduler. Because
	// we're requesting this one minute after the operations were
	// created, we should gradually see this list shrink. Eventually
	// all invocations should be removed.
	initialSizeClassLearner.EXPECT().Abandoned()
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "main",
			Platform:           platform,
		},
	}
	for i := 0; i <= len(operationParameters); i++ {
		clock.EXPECT().Now().Return(time.Unix(1069+int64(i), 0)).Times(3)

		invocationStates, err := buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_ALL,
		})
		require.NoError(t, err)
		require.Len(t, invocationStates.Invocations, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
		})
		require.NoError(t, err)
		require.Len(t, invocationStates.Invocations, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
			SizeClassQueueName: sizeClassQueueName,
			Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
		})
		require.NoError(t, err)
		require.Empty(t, invocationStates.Invocations)
	}
}

func TestInMemoryBuildQueuePreferBeingIdle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// Let a client enqueue an operation.
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: platform,
	}, buffer.UserProvided))
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{0}).
		Return(0, 30*time.Minute, initialSizeClassLearner)
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	timer := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("b9bb6e2c-04ff-4fbd-802b-105be93a8fb7"))
	stream, err := executionClient.Execute(
		ctx,
		&remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
	require.NoError(t, err)
	update, err := stream.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "b9bb6e2c-04ff-4fbd-802b-105be93a8fb7",
		Metadata: metadata,
	}, update)

	// Let a worker pick up the operation.
	clock.EXPECT().Now().Return(time.Unix(1002, 0)).Times(2)
	timer = mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
	timer.EXPECT().Stop().Return(true)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	// The client should be informed the operation has started executing.
	update, err = stream.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "b9bb6e2c-04ff-4fbd-802b-105be93a8fb7",
		Metadata: metadata,
	}, update)

	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 1800},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response)

	// Let the worker complete the execution of the operation.
	// Normally this would be a blocking call, as it would wait
	// until more work is available. However, because
	// PreferBeingIdle is set, the call will return immediately,
	// explicitly forcing the worker to the idle state. This allows
	// workers to terminate gracefully.
	initialSizeClassLearner.EXPECT().Succeeded(10*time.Second, []uint32{0})
	clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(3)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									ExecutionStartTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590687,
									},
									ExecutionCompletedTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590697,
									},
								},
							},
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1003},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// The client should be informed the operation has completed. This
	// should be the last message to be returned.
	update, err = stream.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				ExecutionStartTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590687,
				},
				ExecutionCompletedTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590697,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "b9bb6e2c-04ff-4fbd-802b-105be93a8fb7",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

func TestInMemoryBuildQueueMultipleSizeClasses(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Register a platform queue that allows workers up to size
	// class 8. The maximum needs to be provided to ensure that the
	// execution strategy remains deterministic.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "aarch64"},
			{Name: "os", Value: "linux"},
		},
	}
	platformHooks := mock.NewMockPlatformHooks(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	require.NoError(t, buildQueue.RegisterPredeclaredPlatformQueue(
		digest.MustNewInstanceName("main"),
		platform,
		/* workerInvocationStickinessLimit = */ 0,
		/* maximumQueuedBackgroundLearningOperations = */ 0,
		/* backgroundLearningOperationPriority = */ 0,
		/* maximumSizeClass = */ 8,
		platformHooks))

	// Workers with a higher size class should be rejected, as no
	// requests will end up getting sent to them.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	_, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          9,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Worker provided size class 9, which exceeds the predeclared maximum of 8"), err)

	// Announce a worker with a smaller size class, which should be
	// permitted.
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1002},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// Let a client enqueue a new operation, which we'll schedule on
	// the smaller size class.
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: platform,
	}, buffer.UserProvided))
	platformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	platformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner1 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{3, 8}).
		Return(0, 7*time.Minute, initialSizeClassLearner1)
	clock.EXPECT().Now().Return(time.Unix(1003, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let the worker for the small size class pick it up.
	timer1.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1004, 0)).Times(2)
	timer2 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer2, nil)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1014},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 420},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1003},
				},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}, update)

	// The action fails on the worker of the small size class,
	// meaning that we retry it on the largest size class. An update
	// should be sent back to the client that the operation has
	// moved back to the QUEUED stage.
	initialSizeClassLearner2 := mock.NewMockLearner(ctrl)
	initialSizeClassLearner1.EXPECT().Failed(false).Return(5*time.Minute, initialSizeClassLearner2)
	timer2.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(2)
	timer3 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer3, nil)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExitCode: 137,
							},
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1005},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let a worker for the largest size class pick it up once more.
	// The client should get notified that the operation is in the
	// EXECUTING stage once again.
	timer3.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(2)
	timer4 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer4, nil)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker456",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          8,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1016},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 300},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1003},
				},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}, update)

	// Let the action succeed on the largest size class. This should
	// cause the executing time on the largest size class to be
	// provided to the learner, and completion to be reported to the
	// client.
	initialSizeClassLearner2.EXPECT().Succeeded(3*time.Second, []uint32{3, 8})
	clock.EXPECT().Now().Return(time.Unix(1019, 0)).Times(3)
	timer4.EXPECT().Stop().Return(true)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker456",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          8,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									ExecutionStartTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590687,
									},
									ExecutionCompletedTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590690,
									},
								},
							},
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1019},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				ExecutionStartTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590687,
				},
				ExecutionCompletedTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590690,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})
}

func TestInMemoryBuildQueueBackgroundRun(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Register a platform queue that allows workers up to size
	// class 8. The maximum needs to be provided to ensure that the
	// execution strategy remains deterministic.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "aarch64"},
			{Name: "os", Value: "linux"},
		},
	}
	platformHooks := mock.NewMockPlatformHooks(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	require.NoError(t, buildQueue.RegisterPredeclaredPlatformQueue(
		digest.MustNewInstanceName("main"),
		platform,
		/* workerInvocationStickinessLimit = */ 0,
		/* maximumQueuedBackgroundLearningOperations = */ 10,
		/* backgroundLearningOperationPriority = */ 100,
		/* maximumSizeClass = */ 8,
		platformHooks))

	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1002},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)

	// Let a client enqueue a new operation, which we'll initially
	// schedule on the largest size class.
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: platform,
	}, buffer.UserProvided))
	platformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("main"), gomock.Any(), nil).Return(&anypb.Any{}, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	platformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	})).Return(initialSizeClassSelector, nil)
	initialSizeClassLearner1 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().Select([]uint32{3, 8}).
		Return(1, 7*time.Minute, initialSizeClassLearner1)
	clock.EXPECT().Now().Return(time.Unix(1003, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// Let a worker for the largest size class pick it up.
	timer1.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1004, 0)).Times(2)
	timer2 := mock.NewMockTimer(ctrl)
	clock.EXPECT().NewTimer(time.Minute).Return(timer2, nil)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker456",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          8,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1014},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout: &durationpb.Duration{Seconds: 420},
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1003},
				},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	}, update)

	// The action succeeds on the worker of the largest size class.
	// In response, request that the same action is rerun on the
	// smaller size class. Because we don't want to leave the client
	// blocked on that, this should be done as part of a separate
	// task.
	initialSizeClassLearner2 := mock.NewMockLearner(ctrl)
	initialSizeClassLearner1.EXPECT().Succeeded(3*time.Second, []uint32{3, 8}).Return(0, 1*time.Minute, initialSizeClassLearner2)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("30326ed7-101a-4bf2-93eb-fcb6e7672415"))
	timer2.EXPECT().Stop().Return(true)
	clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(3)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker456",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          8,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									ExecutionStartTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590687,
									},
									ExecutionCompletedTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590690,
									},
								},
							},
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1005},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				ExecutionStartTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590687,
				},
				ExecutionCompletedTimestamp: &timestamppb.Timestamp{
					Seconds: 1620590690,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})

	// Let the worker for the smaller size class pick up the
	// background task. The action should be identical to the
	// original one, except that the timeout is altered and
	// do_not_cache is set.
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1016},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
							SizeBytes: 456,
						},
						Timeout:    &durationpb.Duration{Seconds: 60},
						DoNotCache: true,
					},
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1003},
				},
			},
		},
	}, response)

	// Let the action succeed on the smaller size class. This should
	// cause the initial size class learner to be finalized.
	initialSizeClassLearner2.EXPECT().Succeeded(3*time.Second, []uint32{3, 8})
	clock.EXPECT().Now().Return(time.Unix(1019, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          3,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									ExecutionStartTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590687,
									},
									ExecutionCompletedTimestamp: &timestamppb.Timestamp{
										Seconds: 1620590690,
									},
								},
							},
						},
					},
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1019},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}, response)
}

func TestInMemoryBuildQueueIdleSynchronizingWorkers(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	mockClock := mock.NewMockClock(ctrl)
	mockClock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, mockClock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Common values used by steps below.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "aarch64"},
			{Name: "os", Value: "linux"},
		},
	}
	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
		DoNotCache: true,
		Timeout:    &durationpb.Duration{Seconds: 420},
	}
	actionDigest := &remoteexecution.Digest{
		Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		SizeBytes: 123,
	}
	invocationID1, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: "33b38903-d456-4417-951b-bd8a2681c136",
	})
	require.NoError(t, err)
	invocationID2, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: "75b27319-2704-4fa0-84c9-6881ea5b93ad",
	})
	require.NoError(t, err)
	invocationID3, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: "d6be714f-cef6-408f-b3c7-dfeeae48f63f",
	})
	require.NoError(t, err)
	workerID1 := map[string]string{
		"hostname": "worker123",
		"thread":   "42",
	}
	workerID2 := map[string]string{
		"hostname": "worker123",
		"thread":   "43",
	}
	sizeClassQueueName := &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: &buildqueuestate.PlatformQueueName{
			Platform: platform,
		},
	}
	metadataExecuting, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage:        remoteexecution.ExecutionStage_EXECUTING,
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	metadataCompleted, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
		Stage:        remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{},
	})
	require.NoError(t, err)

	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
	).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided)).AnyTimes()
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: platform,
	}, buffer.UserProvided)).AnyTimes()

	// Create a worker that does a blocking Synchronize() call
	// against the scheduler.
	mockClock.EXPECT().Now().Return(time.Unix(1000, 0))
	timer1 := mock.NewMockTimer(ctrl)
	wait1 := make(chan struct{}, 1)
	mockClock.EXPECT().NewTimer(time.Minute).DoAndReturn(func(d time.Duration) (clock.Timer, chan<- time.Time) {
		wait1 <- struct{}{}
		return timer1, nil
	})
	var response1 *remoteworker.SynchronizeResponse
	var err1 error
	wait2 := make(chan struct{}, 1)
	go func() {
		response1, err1 = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: workerID1,
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		})
		wait2 <- struct{}{}
	}()
	<-wait1

	// Assign a task to it. The worker should be woken up directly.
	// The client should immediately receive an EXECUTING update.
	// There is no need to return QUEUED.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.EmptyInstanceName, gomock.Any(), nil).Return(invocationID1, nil)
	initialSizeClassSelector1 := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector1, nil)
	initialSizeClassLearner1 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector1.EXPECT().Select([]uint32{0}).Return(0, 7*time.Minute, initialSizeClassLearner1)
	mockClock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
	timer2 := mock.NewMockTimer(ctrl)
	mockClock.EXPECT().NewTimer(time.Minute).Return(timer2, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	timer1.EXPECT().Stop()

	stream1, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	update, err := stream1.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadataExecuting,
	})

	// The worker should get unblocked.
	<-wait2
	require.NoError(t, err1)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1011},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest:    actionDigest,
					Action:          action,
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response1)

	// Let the worker complete the operation. This should wake up
	// the client.
	mockClock.EXPECT().Now().Return(time.Unix(1002, 0)).Times(3)
	initialSizeClassLearner1.EXPECT().Succeeded(time.Duration(0), []uint32{0})
	timer2.EXPECT().Stop()
	timer3 := mock.NewMockTimer(ctrl)
	wait3 := make(chan struct{}, 1)
	mockClock.EXPECT().NewTimer(time.Minute).DoAndReturn(func(d time.Duration) (clock.Timer, chan<- time.Time) {
		wait3 <- struct{}{}
		return timer3, nil
	})
	var response2 *remoteworker.SynchronizeResponse
	var err2 error
	wait4 := make(chan struct{}, 1)
	go func() {
		response2, err2 = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: workerID1,
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Executing_{
					Executing: &remoteworker.CurrentState_Executing{
						ActionDigest: actionDigest,
						ExecutionState: &remoteworker.CurrentState_Executing_Completed{
							Completed: &remoteexecution.ExecuteResponse{
								Result: &remoteexecution.ActionResult{},
							},
						},
					},
				},
			},
		})
		wait4 <- struct{}{}
	}()
	<-wait3

	update, err = stream1.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadataCompleted,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})
	_, err = stream1.Recv()
	require.Equal(t, io.EOF, err)

	// Even though there are no longer any active or queued
	// invocations, the scheduler should keep track of the
	// invocation belonging to the previously completed action,
	// keeping track of how which workers are associated with it.
	mockClock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(3)
	invocationStates, err := buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ALL,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListInvocationsResponse{
		Invocations: []*buildqueuestate.InvocationState{
			{
				Id:                            invocationID1,
				IdleWorkersCount:              1,
				IdleSynchronizingWorkersCount: 1,
			},
		},
	}, invocationStates)

	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ACTIVE,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListInvocationsResponse{}, invocationStates)

	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_QUEUED,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListInvocationsResponse{}, invocationStates)

	// Create a second worker that issues a blocking Synchronize()
	// call against the scheduler.
	mockClock.EXPECT().Now().Return(time.Unix(1004, 0))
	timer4 := mock.NewMockTimer(ctrl)
	wait5 := make(chan struct{}, 1)
	mockClock.EXPECT().NewTimer(time.Minute).DoAndReturn(func(d time.Duration) (clock.Timer, chan<- time.Time) {
		wait5 <- struct{}{}
		return timer4, nil
	})
	var response3 *remoteworker.SynchronizeResponse
	var err3 error
	wait6 := make(chan struct{}, 1)
	go func() {
		response3, err3 = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: workerID2,
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		})
		wait6 <- struct{}{}
	}()
	<-wait5

	// Schedule another operation. Because this operation uses a
	// different invocation ID, it must be scheduled on the second
	// worker. We want to keep the first worker available for
	// actions for the same invocation ID.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.EmptyInstanceName, gomock.Any(), nil).Return(invocationID2, nil)
	initialSizeClassSelector2 := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector2, nil)
	initialSizeClassLearner2 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector2.EXPECT().Select([]uint32{0}).Return(0, 7*time.Minute, initialSizeClassLearner2)
	mockClock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(2)
	timer5 := mock.NewMockTimer(ctrl)
	mockClock.EXPECT().NewTimer(time.Minute).Return(timer5, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("e98bb734-0ec7-4cc5-bb98-bb3d5c0788c2"))
	timer4.EXPECT().Stop()

	stream2, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	update, err = stream2.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "e98bb734-0ec7-4cc5-bb98-bb3d5c0788c2",
		Metadata: metadataExecuting,
	})

	<-wait6
	require.NoError(t, err3)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1015},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest:    actionDigest,
					Action:          action,
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1005},
				},
			},
		},
	}, response3)

	// Let the second worker complete the operation.
	mockClock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(3)
	initialSizeClassLearner2.EXPECT().Succeeded(time.Duration(0), []uint32{0})
	timer5.EXPECT().Stop()
	timer6 := mock.NewMockTimer(ctrl)
	wait7 := make(chan struct{}, 1)
	mockClock.EXPECT().NewTimer(time.Minute).DoAndReturn(func(d time.Duration) (clock.Timer, chan<- time.Time) {
		wait7 <- struct{}{}
		return timer6, nil
	})
	var response4 *remoteworker.SynchronizeResponse
	var err4 error
	wait8 := make(chan struct{}, 1)
	go func() {
		response4, err4 = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: workerID2,
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Executing_{
					Executing: &remoteworker.CurrentState_Executing{
						ActionDigest: actionDigest,
						ExecutionState: &remoteworker.CurrentState_Executing_Completed{
							Completed: &remoteexecution.ExecuteResponse{
								Result: &remoteexecution.ActionResult{},
							},
						},
					},
				},
			},
		})
		wait8 <- struct{}{}
	}()
	<-wait7

	update, err = stream2.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "e98bb734-0ec7-4cc5-bb98-bb3d5c0788c2",
		Metadata: metadataCompleted,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})
	_, err = stream1.Recv()
	require.Equal(t, io.EOF, err)

	// Both invocations should now have one worker.
	mockClock.EXPECT().Now().Return(time.Unix(1007, 0))
	invocationStates, err = buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		SizeClassQueueName: sizeClassQueueName,
		Filter:             buildqueuestate.ListInvocationsRequest_ALL,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &buildqueuestate.ListInvocationsResponse{
		Invocations: []*buildqueuestate.InvocationState{
			{
				Id:                            invocationID1,
				IdleWorkersCount:              1,
				IdleSynchronizingWorkersCount: 1,
			},
			{
				Id:                            invocationID2,
				IdleWorkersCount:              1,
				IdleSynchronizingWorkersCount: 1,
			},
		},
	}, invocationStates)

	// When submitting a third operation that uses an unknown
	// invocation, we should execute it on the first worker, as that
	// worker is associated with an invocation that is least
	// recently seen.
	defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.EmptyInstanceName, gomock.Any(), nil).Return(invocationID3, nil)
	initialSizeClassSelector3 := mock.NewMockSelector(ctrl)
	defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector3, nil)
	initialSizeClassLearner3 := mock.NewMockLearner(ctrl)
	initialSizeClassSelector3.EXPECT().Select([]uint32{0}).Return(0, 7*time.Minute, initialSizeClassLearner3)
	mockClock.EXPECT().Now().Return(time.Unix(1008, 0)).Times(2)
	timer7 := mock.NewMockTimer(ctrl)
	mockClock.EXPECT().NewTimer(time.Minute).Return(timer7, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("a0942b25-9c84-42da-93cb-cbd16cf61917"))
	timer3.EXPECT().Stop()

	stream3, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	update, err = stream3.Recv()
	require.NoError(t, err)
	testutil.RequireEqualProto(t, update, &longrunning.Operation{
		Name:     "a0942b25-9c84-42da-93cb-cbd16cf61917",
		Metadata: metadataExecuting,
	})

	<-wait4
	require.NoError(t, err2)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1018},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &remoteworker.DesiredState_Executing{
					ActionDigest:    actionDigest,
					Action:          action,
					QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1008},
				},
			},
		},
	}, response2)
}

func TestInMemoryBuildQueueWorkerInvocationStickinessLimit(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, allowAllAuthorizer())
	executionClient := getExecutionClient(t, buildQueue)

	// Register a platform queue that has a small amount of worker
	// invocation stickiness. This should workers to prefer picking
	// up operations belonging to the same invocation as the last
	// executed task, if the difference in queueing time is small.
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "aarch64"},
			{Name: "os", Value: "linux"},
		},
	}
	platformHooks := mock.NewMockPlatformHooks(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	require.NoError(t, buildQueue.RegisterPredeclaredPlatformQueue(
		digest.EmptyInstanceName,
		platform,
		/* workerInvocationStickinessLimit = */ 3*time.Second,
		/* maximumQueuedBackgroundLearningOperations = */ 10,
		/* backgroundLearningOperationPriority = */ 100,
		/* maximumSizeClass = */ 0,
		platformHooks))

	operationParameters := []struct {
		operationName    string
		toolInvocationID string
	}{
		{"716f2e32-d273-49e7-a842-82282e89d1a3", "76d8f589-8d22-4541-a612-39e4c670e531"},
		{"d354a1ea-0945-4c84-be17-ac01b8058d06", "0175ce91-1363-4cc9-8eb9-e67e82f9fdbb"},
		{"0a656823-9f91-4220-9dcd-58503e62e6e8", "76d8f589-8d22-4541-a612-39e4c670e531"},
		{"175ead9a-a095-43a5-9f2c-e3a293938ff3", "76d8f589-8d22-4541-a612-39e4c670e531"},
	}
	type streamHandle struct {
		timer                   *mock.MockTimer
		stream                  remoteexecution.Execution_ExecuteClient
		initialSizeClassLearner *mock.MockLearner
	}
	var streamHandles []streamHandle

	// Schedule some actions, all two seconds apart. They belong to
	// two different invocation IDs.
	for i, p := range operationParameters {
		action := &remoteexecution.Action{
			DoNotCache: true,
			CommandDigest: &remoteexecution.Digest{
				Hash:      "9b818e201c59f31954cb1e126cc67562ec545ab4",
				SizeBytes: 456,
			},
		}
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("", "0474d2f48968a56da4de20718d8ac23aafd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("", "9b818e201c59f31954cb1e126cc67562ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: platform,
		}, buffer.UserProvided))
		requestMetadata := &remoteexecution.RequestMetadata{
			ToolInvocationId: p.toolInvocationID,
		}
		requestMetadataAny, err := anypb.New(requestMetadata)
		require.NoError(t, err)
		platformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.EmptyInstanceName, gomock.Any(), nil).Return(requestMetadataAny, nil)
		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		platformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector, nil)
		initialSizeClassLearner := mock.NewMockLearner(ctrl)
		initialSizeClassSelector.EXPECT().Select([]uint32{0}).
			Return(0, time.Minute, initialSizeClassLearner)
		clock.EXPECT().Now().Return(time.Unix(1010+int64(i)*2, 0))
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		uuidGenerator.EXPECT().Call().Return(uuid.Parse(p.operationName))

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     p.operationName,
			Metadata: metadata,
		})

		streamHandles = append(streamHandles, streamHandle{
			timer:                   timer,
			stream:                  stream,
			initialSizeClassLearner: initialSizeClassLearner,
		})
	}

	// Let a worker run the actions sequentially. The order in which
	// execution takes place differs from the queueing order,
	// because the third action is permitted to run before the
	// second. The fourth action is not, because its queueing
	// timestamp exceeds what's permitted by the worker invocation
	// stickiness.
	for i, operationIndex := range []int{0, 2, 1, 3} {
		// Starting execution should cause the client to receive
		// an EXECUTING message.
		clock.EXPECT().Now().Return(time.Unix(1020+int64(i)*2, 0)).Times(2)
		streamHandles[operationIndex].timer.EXPECT().Stop()
		timer := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)
		response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1030 + int64(i)*2},
			DesiredState: &remoteworker.DesiredState{
				WorkerState: &remoteworker.DesiredState_Executing_{
					Executing: &remoteworker.DesiredState_Executing{
						ActionDigest: &remoteexecution.Digest{
							Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
							SizeBytes: 123,
						},
						Action: &remoteexecution.Action{
							CommandDigest: &remoteexecution.Digest{
								Hash:      "9b818e201c59f31954cb1e126cc67562ec545ab4",
								SizeBytes: 456,
							},
							DoNotCache: true,
							Timeout:    &durationpb.Duration{Seconds: 60},
						},
						QueuedTimestamp: &timestamppb.Timestamp{Seconds: 1010 + int64(operationIndex)*2},
					},
				},
			},
		}, response)

		stream := streamHandles[operationIndex].stream
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_EXECUTING,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		operationName := operationParameters[operationIndex].operationName
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     operationName,
			Metadata: metadata,
		})

		// Finishing execution should cause the client to
		// receive a COMPLETED message.
		streamHandles[operationIndex].initialSizeClassLearner.EXPECT().Succeeded(time.Duration(0), []uint32{0})
		clock.EXPECT().Now().Return(time.Unix(1021+int64(i)*2, 0)).Times(3)
		timer.EXPECT().Stop()
		response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			Platform: platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Executing_{
					Executing: &remoteworker.CurrentState_Executing{
						ActionDigest: &remoteexecution.Digest{
							Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
							SizeBytes: 123,
						},
						ExecutionState: &remoteworker.CurrentState_Executing_Completed{
							Completed: &remoteexecution.ExecuteResponse{
								Result: &remoteexecution.ActionResult{},
							},
						},
						PreferBeingIdle: true,
					},
				},
			},
		})

		update, err = stream.Recv()
		require.NoError(t, err)
		metadata, err = anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_COMPLETED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0474d2f48968a56da4de20718d8ac23aafd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		executeResponse, err := anypb.New(&remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     operationName,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: executeResponse},
		})
	}
}

func TestInMemoryBuildQueueAuthorization(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0)).AnyTimes()
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	defaultPlatformHooks := mock.NewMockPlatformHooks(ctrl)
	authorizer := mock.NewMockAuthorizer(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000, defaultPlatformHooks, authorizer)
	beepboop := digest.MustNewInstanceName("beepboop")

	t.Run("GetCapabilities-NotAuthorized", func(t *testing.T) {
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{beepboop}).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})
		capabilities, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "beepboop",
		})
		require.NoError(t, err)
		require.False(t, capabilities.ExecutionCapabilities.ExecEnabled)
	})

	t.Run("GetCapabilities-Error", func(t *testing.T) {
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{beepboop}).Return([]error{status.Error(codes.Internal, "I fell over")})
		_, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "beepboop",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Authorization: I fell over"), err)
	})

	t.Run("Execute-NotAuthorized", func(t *testing.T) {
		executionClient := getExecutionClient(t, buildQueue)
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{beepboop}).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})
		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
			InstanceName: "beepboop",
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), err)
	})

	t.Run("WaitExecution", func(t *testing.T) {
		buildQueue.RegisterPredeclaredPlatformQueue(digest.MustNewInstanceName(""), &remoteexecution.Platform{}, 0*time.Second, 0, 0, 0, defaultPlatformHooks)

		// Allow the Execute
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{beepboop}).Return([]error{nil})

		action := &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
			Platform: &remoteexecution.Platform{},
		}

		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("beepboop", "61c585c297d00409bd477b6b80759c94ec545ab4", 456),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))

		contentAddressableStorage.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("beepboop", "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
			Platform: &remoteexecution.Platform{},
		}, buffer.UserProvided))

		defaultPlatformHooks.EXPECT().ExtractInvocationID(gomock.Any(), digest.MustNewInstanceName("beepboop"), gomock.Any(), nil).Return(&anypb.Any{}, nil)

		initialSizeClassSelector := mock.NewMockSelector(ctrl)
		defaultPlatformHooks.EXPECT().Analyze(gomock.Any(), gomock.Any(), testutil.EqProto(t, action)).Return(initialSizeClassSelector, nil)

		initialSizeClassLearner := mock.NewMockLearner(ctrl)
		initialSizeClassSelector.EXPECT().Select([]uint32{0}).Return(0, 30*time.Minute, initialSizeClassLearner)

		timer1 := mock.NewMockTimer(ctrl)
		clock.EXPECT().NewTimer(time.Minute).Return(timer1, nil)

		uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

		executionClient := getExecutionClient(t, buildQueue)

		// Error on the WaitExecution
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{beepboop}).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})
		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
			InstanceName: "beepboop",
		})
		require.NoError(t, err)
		update, err := stream.Recv()
		require.NoError(t, err)
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
				SizeBytes: 456,
			},
		})
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
			Metadata: metadata,
		})

		stream2, err := executionClient.WaitExecution(ctx, &remoteexecution.WaitExecutionRequest{
			Name: "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		})
		require.NoError(t, err)
		_, err = stream2.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), err)
	})
}

func allowAllAuthorizer() auth.Authorizer {
	return auth.NewStaticAuthorizer(func(digest.InstanceName) bool { return true })
}

// TODO: Make testing coverage of InMemoryBuildQueue complete.
