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
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/longrunning"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		testutil.RequireEqualStatus(t, err, status.Error(codes.Unavailable, "No workers exist for instance \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"))
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
		testutil.RequireEqualStatus(t, err, status.Error(codes.FailedPrecondition, "No workers exist for instance \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"))
	})
}

func TestInMemoryBuildQueuePurgeStaleWorkersAndQueues(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// Let a client enqueue a new operation.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	timer1 := mock.NewMockTimer(ctrl)
	wakeup1 := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, wakeup1)
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012},
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
					},
					Command:         &remoteexecution.Command{},
					QueuedTimestamp: &timestamp.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response)

	// The next time the client receives an update on the operation,
	// it should be in the EXECUTING state.
	timer2 := mock.NewMockTimer(ctrl)
	wakeup2 := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer2, wakeup2)
	wakeup1 <- time.Unix(1061, 0)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_EXECUTING,
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

	// Because the worker is not providing any updates, the
	// operation should be terminated.
	// TODO: This could already trigger as soon as 1062, but would
	// require waitExecution() to do a short sleep, which may
	// increase complexity/overhead.
	clock.EXPECT().Now().Return(time.Unix(1121, 0))
	wakeup2 <- time.Unix(1121, 0)
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
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
		metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_QUEUED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, update, &longrunning.Operation{
			Name:     fakeUUID,
			Metadata: metadata,
		})
		streams = append(streams, stream)
	}

	// After workers are absent for long enough, the corresponding
	// platform queue is also garbage collected.
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
	testutil.RequireEqualStatus(t, err, status.Error(codes.FailedPrecondition, "No workers exist for instance \"main\" platform {}"))

	// Operations that were queued should have been cancelled when
	// the platform queue was garbage collected. All eight should
	// get woken up.
	for i, fakeUUID := range fakeUUIDs {
		update, err = streams[i].Recv()
		require.NoError(t, err)
		metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_COMPLETED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		executeResponse, err = ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
			Status: status.New(codes.Unavailable, "Workers for this instance name and platform disappeared while task was queued").Proto(),
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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	for i := 0; i < 2; i++ {
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
	}
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
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
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	clock.EXPECT().Now().Return(time.Unix(1080, 0))
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
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	clock.EXPECT().Now().Return(time.Unix(1080, 0))
	allOperations, paginationInfo := buildQueue.ListDetailedOperationState(10, nil)
	testutil.RequireEqualProto(t, &remoteexecution.Digest{
		Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		SizeBytes: 123,
	}, allOperations[0].ActionDigest)
	allOperations[0].ActionDigest = nil
	require.Equal(t, allOperations, []re_builder.DetailedOperationState{
		{
			BasicOperationState: re_builder.BasicOperationState{
				Name:            "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
				QueuedTimestamp: time.Unix(1070, 0).UTC(),
			},
			InstanceName: digest.MustNewInstanceName("main"),
			Stage:        remoteexecution.ExecutionStage_QUEUED,
		},
	})
	require.Equal(t, paginationInfo, re_builder.PaginationInfo{
		StartIndex:   0,
		EndIndex:     1,
		TotalEntries: 1,
	})

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
	allOperations, paginationInfo = buildQueue.ListDetailedOperationState(10, nil)
	testutil.RequireEqualProto(t, &remoteexecution.Digest{
		Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		SizeBytes: 123,
	}, allOperations[0].ActionDigest)
	allOperations[0].ActionDigest = nil
	timeout := time.Unix(1150, 0)
	require.Equal(t, allOperations, []re_builder.DetailedOperationState{
		{
			BasicOperationState: re_builder.BasicOperationState{
				Name:            "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
				QueuedTimestamp: time.Unix(1070, 0).UTC(),
				Timeout:         &timeout,
			},
			InstanceName: digest.MustNewInstanceName("main"),
			Stage:        remoteexecution.ExecutionStage_QUEUED,
		},
	})
	require.Equal(t, paginationInfo, re_builder.PaginationInfo{
		StartIndex:   0,
		EndIndex:     1,
		TotalEntries: 1,
	})

	// And it should be gone after it.
	clock.EXPECT().Now().Return(time.Unix(1150, 0))
	allOperations, paginationInfo = buildQueue.ListDetailedOperationState(10, nil)
	require.Empty(t, allOperations)
	require.Equal(t, paginationInfo, re_builder.PaginationInfo{
		StartIndex:   0,
		EndIndex:     0,
		TotalEntries: 0,
	})
}

func TestInMemoryBuildQueueKillOperation(t *testing.T) {
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
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
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	for i := int64(0); i < 10; i++ {
		clock.EXPECT().Now().Return(time.Unix(1002+i, 0))
		response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			InstanceName: "main",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "cpu", Value: "armv6"},
					{Name: "os", Value: "linux"},
				},
			},
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &empty.Empty{},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012 + i},
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
						},
						Command: &remoteexecution.Command{
							Platform: &remoteexecution.Platform{
								Properties: []*remoteexecution.Platform_Property{
									{Name: "cpu", Value: "armv6"},
									{Name: "os", Value: "linux"},
								},
							},
						},
						QueuedTimestamp: &timestamp.Timestamp{Seconds: 1001},
					},
				},
			},
		}, response)
	}

	// Requesting the same operation too many times should cause the
	// scheduler to give up on handing out the same operation. We
	// don't want a single operation to crash-loop a worker
	// indefinitely.
	clock.EXPECT().Now().Return(time.Unix(1012, 0)).Times(3)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
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
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// The client should be informed that the operation causes the
	// worker to crash-loop.
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
	executionClient := getExecutionClient(t, buildQueue)

	// Announce a new worker, which creates a queue for operations.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
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
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// Let one client enqueue an operation.
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012},
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
					},
					Command: &remoteexecution.Command{
						Platform: &remoteexecution.Platform{
							Properties: []*remoteexecution.Platform_Property{
								{Name: "cpu", Value: "armv6"},
								{Name: "os", Value: "linux"},
							},
						},
					},
					QueuedTimestamp: &timestamp.Timestamp{Seconds: 1001},
				},
			},
		},
	}, response)

	// Kill the operation.
	clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(3)
	require.True(t, buildQueue.KillOperation("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

	// The client should be informed that the operation was killed.
	update, err = stream1.Recv()
	require.NoError(t, err)
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
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
		InstanceName: "main",
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
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)

	// When no work appears, workers should still be woken up
	// periodically to resynchronize. This ensures that workers that
	// disappear without closing their TCP connections are purged
	// quickly.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	timer := mock.NewMockTimer(ctrl)
	timerChannel := make(chan time.Time, 1)
	timerChannel <- time.Unix(1060, 0)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, timerChannel)
	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "cpu", Value: "armv6"},
				{Name: "os", Value: "linux"},
			},
		},
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1060},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// The worker should not be drained by default.
	instanceName := digest.MustNewInstanceName("main")
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	workerState, _, err := buildQueue.ListWorkerState(instanceName, platform, false, 1000, nil)
	require.NoError(t, err)
	timeout := time.Unix(1060, 0)
	require.Equal(t, []re_builder.WorkerState{
		{
			WorkerID: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			Timeout: &timeout,
			Drained: false,
		},
	}, workerState)

	// Adding a drain that doesn't match the worker should cause no
	// changes.
	clock.EXPECT().Now().Return(time.Unix(1003, 0))
	err = buildQueue.AddDrain(instanceName, platform, map[string]string{
		"hostname": "worker124",
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	workerState, _, err = buildQueue.ListWorkerState(instanceName, platform, false, 1000, nil)
	require.NoError(t, err)
	require.Equal(t, []re_builder.WorkerState{
		{
			WorkerID: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			Timeout: &timeout,
			Drained: false,
		},
	}, workerState)

	// Adding a drain that does match the worker should cause it to
	// be reported as if being drained.
	clock.EXPECT().Now().Return(time.Unix(1005, 0))
	err = buildQueue.AddDrain(instanceName, platform, map[string]string{
		"hostname": "worker123",
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	workerState, _, err = buildQueue.ListWorkerState(instanceName, platform, false, 1000, nil)
	require.NoError(t, err)
	require.Equal(t, []re_builder.WorkerState{
		{
			WorkerID: map[string]string{
				"hostname": "worker123",
				"thread":   "42",
			},
			Timeout: &timeout,
			Drained: true,
		},
	}, workerState)

	// Enqueue an operation.
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	timer1 := mock.NewMockTimer(ctrl)
	wakeup1 := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer1, wakeup1)
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1008},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	// Remove the drain. The scheduler should now return the
	// operation if requested.
	clock.EXPECT().Now().Return(time.Unix(1009, 0))
	err = buildQueue.RemoveDrain(instanceName, platform, map[string]string{
		"hostname": "worker123",
	})
	require.NoError(t, err)
	clock.EXPECT().Now().Return(time.Unix(1010, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1020},
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
					},
					Command: &remoteexecution.Command{
						Platform: &remoteexecution.Platform{
							Properties: []*remoteexecution.Platform_Property{
								{Name: "cpu", Value: "armv6"},
								{Name: "os", Value: "linux"},
							},
						},
					},
					QueuedTimestamp: &timestamp.Timestamp{Seconds: 1007},
				},
			},
		},
	}, response)
}

func TestInMemoryBuildQueueInvocationFairness(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
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

		requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		})
		require.NoError(t, err)

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
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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

	// Check that ListInvocationState() reports all five
	// invocations, both when justQueuedInvocations is true and
	// false. When true, the invocations should be returned in
	// scheduling order. Otherwise, they should be returned
	// alphabetically.
	clock.EXPECT().Now().Return(time.Unix(1036, 0))
	invocationStates, err := buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		true)
	require.NoError(t, err)
	require.Len(t, invocationStates, 5)
	for i, invocationID := range []string{
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
	} {
		require.Equal(t, invocationID, invocationStates[i].InvocationID)
		require.Equal(t, 5, invocationStates[i].QueuedOperationsCount)
		require.Equal(t, 0, invocationStates[i].ExecutingOperationsCount)
	}

	clock.EXPECT().Now().Return(time.Unix(1036, 0))
	invocationStates, err = buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		false)
	require.NoError(t, err)
	require.Len(t, invocationStates, 5)
	for i, invocationID := range []string{
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
	} {
		require.Equal(t, invocationID, invocationStates[i].InvocationID)
		require.Equal(t, 5, invocationStates[i].QueuedOperationsCount)
		require.Equal(t, 0, invocationStates[i].ExecutingOperationsCount)
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
		clock.EXPECT().Now().Return(time.Unix(1040, 0))
		response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
			WorkerId: map[string]string{
				"hostname": "worker123",
				"thread":   strconv.FormatInt(int64(i), 10),
			},
			InstanceName: "main",
			Platform:     platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &empty.Empty{},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1050},
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
						},
						Command: &remoteexecution.Command{
							Platform: platform,
						},
						QueuedTimestamp: &timestamp.Timestamp{Seconds: 1010 + int64(i)},
					},
				},
			},
		}, response)
	}

	// Call ListInvocationState() again. All operations should now
	// be reported as executing, instead of being queued.
	clock.EXPECT().Now().Return(time.Unix(1041, 0))
	invocationStates, err = buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		true)
	require.NoError(t, err)
	require.Empty(t, invocationStates)

	clock.EXPECT().Now().Return(time.Unix(1042, 0))
	invocationStates, err = buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		false)
	require.NoError(t, err)
	require.Len(t, invocationStates, 5)
	for i, invocationID := range []string{
		"351fdffe-04df-4fe0-98c7-b4f5a463fd52",
		"4712de52-4518-4840-91d5-c9e13a38cc5a",
		"4fd2080e-d805-4d68-ab51-460106c8f372",
		"66275a66-8aad-498a-9b4a-26b4f5a66789",
		"dfe3ca8b-64bc-4efd-b8a9-2bdc3827f0ac",
	} {
		require.Equal(t, invocationID, invocationStates[i].InvocationID)
		require.Equal(t, 0, invocationStates[i].QueuedOperationsCount)
		require.Equal(t, 5, invocationStates[i].ExecutingOperationsCount)
	}

	// Call ListInvocationState() a final time after letting a
	// sufficient amount of time pass. This should cause all workers
	// to be removed from the scheduler, as they didn't provide any
	// updates. All associated operations should be completed,
	// meaning that no invocations will be reported.
	clock.EXPECT().Now().Return(time.Unix(1200, 0)).Times(51)
	invocationStates, err = buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		true)
	require.NoError(t, err)
	require.Empty(t, invocationStates)

	clock.EXPECT().Now().Return(time.Unix(1200, 0))
	invocationStates, err = buildQueue.ListInvocationState(
		digest.MustNewInstanceName("main"),
		platform,
		false)
	require.NoError(t, err)
	require.Empty(t, invocationStates)

	// All clients should receive an error that their operations
	// terminated due to the loss of workers.
	for i, p := range operationParameters {
		update, err := streams[i].Recv()
		require.NoError(t, err)
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage: remoteexecution.ExecutionStage_COMPLETED,
			ActionDigest: &remoteexecution.Digest{
				Hash:      p.actionHash,
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		executeResponse, err := ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	}, response)

	// Let ten workers create ten operations. Because they all refer
	// to the same action, all requests should be deduplicated into
	// the same task.
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

		requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		})
		require.NoError(t, err)

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
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	for i := 0; i <= len(operationParameters); i++ {
		clock.EXPECT().Now().Return(time.Unix(1069+int64(i), 0)).Times(2)

		invocationStates, err := buildQueue.ListInvocationState(
			digest.MustNewInstanceName("main"),
			platform,
			false)
		require.NoError(t, err)
		require.Len(t, invocationStates, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocationState(
			digest.MustNewInstanceName("main"),
			platform,
			true)
		require.NoError(t, err)
		require.Len(t, invocationStates, len(operationParameters)-i)
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
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	}, response)

	// Let ten workers create ten operations. Because they all refer
	// to the same action, all requests should be deduplicated into
	// the same task.
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

		requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
			ToolInvocationId: p.invocationID,
		})
		require.NoError(t, err)

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
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1075},
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
					},
					Command: &remoteexecution.Command{
						Platform: platform,
					},
					QueuedTimestamp: &timestamp.Timestamp{Seconds: 1010},
				},
			},
		},
	}, response)

	// Get listings of invocations known by the scheduler. Because
	// we're requesting this one minute after the operations were
	// created, we should gradually see this list shrink. Eventually
	// all invocations should be removed.
	for i := 0; i <= len(operationParameters); i++ {
		clock.EXPECT().Now().Return(time.Unix(1069+int64(i), 0)).Times(2)

		invocationStates, err := buildQueue.ListInvocationState(
			digest.MustNewInstanceName("main"),
			platform,
			false)
		require.NoError(t, err)
		require.Len(t, invocationStates, len(operationParameters)-i)

		invocationStates, err = buildQueue.ListInvocationState(
			digest.MustNewInstanceName("main"),
			platform,
			true)
		require.NoError(t, err)
		require.Empty(t, invocationStates)
	}
}

func TestInMemoryBuildQueuePreferBeingIdle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	buildQueue := re_builder.NewInMemoryBuildQueue(contentAddressableStorage, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, 10000)
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
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "099a3f6dc1e8e91dbcca4ea964cd2237d4b11733",
						SizeBytes: 123,
					},
					ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
						FetchingInputs: &empty.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
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
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceName: "main",
		Platform:     platform,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1012},
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
					},
					Command: &remoteexecution.Command{
						Platform: platform,
					},
					QueuedTimestamp: &timestamp.Timestamp{Seconds: 1001},
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
	clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(3)
	response, err = buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
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
					PreferBeingIdle: true,
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1003},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	}, response)

	// The client should be informed the operation has been
	// completed. This should be the last message to be returned.
	update, err = stream.Recv()
	require.NoError(t, err)
	metadata, err = ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_COMPLETED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	executeResponse, err := ptypes.MarshalAny(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{},
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

// TODO: Make testing coverage of InMemoryBuildQueue complete.
