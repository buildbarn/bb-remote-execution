package builder_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_builder "github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
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
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var (
	buildQueueConfigurationForTesting = re_builder.InMemoryBuildQueueConfiguration{
		ExecutionUpdateInterval:              time.Minute,
		OperationWithNoWaitersTimeout:        time.Minute,
		PlatformQueueWithNoWorkersTimeout:    15 * time.Minute,
		BusyWorkerSynchronizationInterval:    10 * time.Second,
		GetIdleWorkerSynchronizationInterval: func() time.Duration { return time.Minute },
		WorkerOperationRetryCount:            9,
		WorkerWithNoSynchronizationsTimeout:  time.Minute,
		InvocationInfoTimeout:                60 * time.Minute,
		RemoteExecutionMetadataKeys:          []string{"key1", "key2"},
	}
)

const INSTANCE_NAME = "main"

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

// Add a dummy invocation ID to a context
func addDummyInvocationIdToContext(ctx context.Context) context.Context {
	return addInvocationIdToContext(ctx, "DUMMY_INVOCATION_ID")
}

// Add an invocation ID to a context
func addInvocationIdToContext(ctx context.Context, invocationID string) context.Context {
	requestMetadata, err := ptypes.MarshalAny(&remoteexecution.RequestMetadata{
		ToolInvocationId: invocationID,
	})
	if err != nil {
		panic(err)
	}
	data := string(requestMetadata.GetValue())
	return metadata.AppendToOutgoingContext(ctx, "build.bazel.remote.execution.v2.requestmetadata-bin", data)
}

// Add a key, value pair to a context
func addKeyValueToContext(ctx context.Context, key string, value string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, key, value)
}

func TestInMemoryBuildQueueExecuteBadRequest(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Failed to extract digest for action: Unknown digest hash length: 24 characters"))
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
		require.Equal(t, err, status.Error(codes.FailedPrecondition, "Failed to obtain action: Blob not found"))
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
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Failed to extract digest for command: Unknown digest hash length: 24 characters"))
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
		require.Equal(t, err, status.Error(codes.FailedPrecondition, "Failed to obtain command: Blob not found"))
	})

	// Remote exection context is missing invocation ID
	t.Run("Missing invocation ID", func(t *testing.T) {
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

		stream, err := executionClient.Execute(baseCtx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Contains(t, err.Error(),
			"Failed to extract invocation ID: Missing remote execution request metadata from bazel client")
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
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Platform properties are not sorted"))
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
		require.Equal(t, err, status.Error(codes.Unavailable, "No workers exist for instance \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"))
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
		require.Equal(t, err, status.Error(codes.FailedPrecondition, "No workers exist for instance \"main\" platform {\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}"))
	})

}

func TestInMemoryBuildQueuePurgeStaleWorkersAndQueues(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, update, &longrunning.Operation{
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
	require.True(t, proto.Equal(response, &remoteworker.SynchronizeResponse{
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
	}))

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
	require.Equal(t, update, &longrunning.Operation{
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
		Status: status.New(codes.Unavailable, "Worker {\"hostname\":\"worker123\",\"thread\":\"42\"} disappeared while operation was executing").Proto(),
	})
	require.NoError(t, err)
	require.Equal(t, update, &longrunning.Operation{
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
		require.Equal(t, update, &longrunning.Operation{
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
	require.Equal(t, err, status.Error(codes.FailedPrecondition, "No workers exist for instance \"main\" platform {}"))

	// Operations that were queued should have been canceled when
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
			Status: status.New(codes.Unavailable, "Workers for this instance and platform disappeared while operation was queued").Proto(),
		})
		require.NoError(t, err)
		require.Equal(t, update, &longrunning.Operation{
			Name:     fakeUUID,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: executeResponse},
		})
	}
}

func TestInMemoryBuildQueuePurgeStaleOperations(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, update, &longrunning.Operation{
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
	require.Equal(t, update, &longrunning.Operation{
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
	require.Equal(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
	})

	// The operation should be present without any timeout
	// associated with it, as there are multiple waiters.
	clock.EXPECT().Now().Return(time.Unix(1080, 0))
	allOperations, paginationInfo, err := buildQueue.ListDetailedOperationState(nil, nil, 10, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(allOperations[0].ActionDigest,
		&remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		}))
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
	allOperations, paginationInfo, err = buildQueue.ListDetailedOperationState(nil, nil, 10, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(allOperations[0].ActionDigest,
		&remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		}))
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

	clock.EXPECT().Now().Return(time.Unix(1149, 999999999))
	invocationID := "DUMMY_INVOCATION_ID"
	allOperationsForInvocationID, _, _ := buildQueue.ListDetailedOperationState(&invocationID, nil, 10, nil)
	allOperationsForInvocationID[0].ActionDigest = nil
	require.Equal(t, allOperations[0], allOperationsForInvocationID[0])

	clock.EXPECT().Now().Return(time.Unix(1149, 999999999))
	otherInvocationID := "OTHER_INVOCATION_ID"
	noOperations, _, _ := buildQueue.ListDetailedOperationState(&otherInvocationID, nil, 10, nil)
	require.Empty(t, noOperations)

	clock.EXPECT().Now().Return(time.Unix(1149, 999999999))
	queued := "Queued"
	queuedOperations, _, _ := buildQueue.ListDetailedOperationState(&invocationID, &queued, 10, nil)
	queuedOperations[0].ActionDigest = nil
	require.Equal(t, allOperations[0], queuedOperations[0])

	clock.EXPECT().Now().Return(time.Unix(1149, 999999999))
	canceled := "Failed"
	canceledOperations, _, _ := buildQueue.ListDetailedOperationState(&invocationID, &canceled, 10, nil)
	require.Empty(t, canceledOperations)

	// And it should be gone after it.
	clock.EXPECT().Now().Return(time.Unix(1150, 0))
	allOperations, paginationInfo, err = buildQueue.ListDetailedOperationState(nil, nil, 10, nil)
	require.NoError(t, err)
	require.Empty(t, allOperations)
	require.Equal(t, paginationInfo, re_builder.PaginationInfo{
		StartIndex:   0,
		EndIndex:     0,
		TotalEntries: 0,
	})
}

func TestInMemoryBuildQueueKillOperation(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, update, &longrunning.Operation{
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
		require.True(t, proto.Equal(response, &remoteworker.SynchronizeResponse{
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
		}))
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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
		Status: status.New(codes.Internal, "Attempted to execute operation 10 times, but it never completed. This operation may cause worker {\"hostname\":\"worker123\",\"thread\":\"42\"} to crash.").Proto(),
	})
	require.NoError(t, err)
	require.Equal(t, update, &longrunning.Operation{
		Name:     "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		Metadata: metadata,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: executeResponse},
	})
}

func TestInMemoryBuildQueueCrashLoopingWorker(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, update, &longrunning.Operation{
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
	require.True(t, proto.Equal(response, &remoteworker.SynchronizeResponse{
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
	}))

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
	require.Equal(t, update, &longrunning.Operation{
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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1060},
	})
}

func TestInMemoryBuildQueueDrainedWorker(t *testing.T) {
	ctrl, baseCtx := gomock.WithContext(context.Background(), t)
	ctx := addDummyInvocationIdToContext(baseCtx)
	defer ctrl.Finish()

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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.Equal(t, update, &longrunning.Operation{
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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
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
	require.True(t, proto.Equal(response, &remoteworker.SynchronizeResponse{
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
	}))
}

func TestInMemoryBuildQueueSortOrderForQueuedOperations(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	buildQueue, clock, platform := setUpQueueWithOperationsForTests(t, ctrl, ctx)

	// Ensure we get the expected numbear of operations, and in the correct order
	clock.EXPECT().Now().Return(time.Unix(1020, 0))
	main, err := digest.NewInstanceName("main")
	require.NoError(t, err)
	enqueued, _, err := buildQueue.ListQueuedOperationState(main, platform, 1024, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 7, len(enqueued))
	uuids := make([]string, len(enqueued))
	for i, v := range enqueued {
		uuids[i] = v.Name
	}
	expectedUuids := [7]string{
		"1a77fe58-b3ae-11ea-b3de-0242ac130004", // Priority HIGH   , Late invocation
		"36ebab65-3c4f-4faf-818b-2eabb4cd1b02", // Priority default, Very early invocation
		"1ef11db1-7b06-44ec-b5d2-af3a4b9a249f", //                 ,
		"250deff8-b3ae-11ea-b3de-0242ac130004", //                 ,
		"d3ecfdda-b3ad-11ea-b3de-0242ac130004", //                 , Early invocation
		"032eaa58-b3ae-11ea-b3de-0242ac130004", //                 , Medium invocation
		"102a980c-b3ae-11ea-b3de-0242ac130004", // Priority LOW    , Very early invocation
	}
	for i, v := range uuids {
		require.Equal(t, expectedUuids[i], v, "Expected %s at index %d but found %s\nExpected UUIDs=%v\nActual UUIDs=%v\n",
			expectedUuids[i], i, v, expectedUuids, uuids)
	}
}

func TestInMemoryBuildQueueListInvocations(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	ctxWithKeyValue := addKeyValueToContext(ctx, "key1", "value1")
	ctxWithKeyValue2 := addKeyValueToContext(ctxWithKeyValue, "key2", "otherValue")
	buildQueue, clock, _ := setUpQueueWithOperationsForTests(t, ctrl, ctxWithKeyValue2)

	// We wont have any inactive invocations
	clock.EXPECT().Now().Return(time.Unix(1020, 0))
	main, err := digest.NewInstanceName("main")
	require.NoError(t, err)
	invocations, _, err := buildQueue.ListInvocations(main, 1024, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(invocations))

	// Ensure we get the expected numbear of operations, and in the correct order
	clock.EXPECT().Now().Return(time.Unix(1021, 0))
	invocations, _, err = buildQueue.ListInvocations(main, 1024, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(invocations))
	expectedKeywords := make(map[string]string, 2)
	expectedKeywords["key1"] = "value1"
	expectedKeywords["key2"] = "otherValue"

	require.Equal(t, int32(-30), invocations[0].Priority)
	require.Equal(t, "LATE", invocations[0].InvocationID)
	require.Equal(t, int(1), invocations[0].QueuedOperationsCount)
	require.Equal(t, expectedKeywords, invocations[0].Keywords)

	require.Equal(t, int32(0), invocations[1].Priority)
	require.Equal(t, "VERY_EARLY", invocations[1].InvocationID)
	require.Equal(t, int(3), invocations[1].QueuedOperationsCount)
	require.Equal(t, expectedKeywords, invocations[1].Keywords)

	require.Equal(t, int32(0), invocations[2].Priority)
	require.Equal(t, "EARLY", invocations[2].InvocationID)
	require.Equal(t, int(2), invocations[2].QueuedOperationsCount)
	require.Equal(t, expectedKeywords, invocations[2].Keywords)

	require.Equal(t, int32(0), invocations[3].Priority)
	require.Equal(t, "MEDIUM", invocations[3].InvocationID)
	require.Equal(t, int(1), invocations[3].QueuedOperationsCount)
	require.Equal(t, expectedKeywords, invocations[3].Keywords)
}

// This sets up the build queue so that it contains 7 operations from 4 different invocations.
func setUpQueueWithOperationsForTests(t *testing.T, ctrl *gomock.Controller, ctx context.Context) (*re_builder.InMemoryBuildQueue, *mock.MockClock, *remoteexecution.Platform) {
	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: "armv6"},
			{Name: "os", Value: "linux"},
		},
	}

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
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
		InstanceName: INSTANCE_NAME,
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
	require.Equal(t, response, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamp.Timestamp{Seconds: 1000},
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &empty.Empty{},
			},
		},
	})

	priorityLow := int32(30)
	priorityDefault := int32(0)
	priorityHigh := int32(-30)
	ctxVeryEarly := addInvocationIdToContext(ctx, "VERY_EARLY")
	ctxEarly := addInvocationIdToContext(ctx, "EARLY")
	ctxMedium := addInvocationIdToContext(ctx, "MEDIUM")
	ctxLate := addInvocationIdToContext(ctx, "LATE")

	// Enqueue an operation with very early invocation time, and default priority.
	enqueueOperation(t, ctrl, ctxVeryEarly, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"da39a3ee5e6b4b0d3255bfef95601890afd80709", 123,
		"61c585c297d00409bd477b6b80759c94ec545ab4", 456,
		"36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		priorityDefault, time.Unix(1001, 0))

	// Enqueue an operation with same invocation time, still default priority.
	enqueueOperation(t, ctrl, ctxVeryEarly, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"442dd07b7ecd17e43f3ad237a35d9120a6383211", 29,
		"2c16adde3b18eab1845a8db2ccae8bdd4627ab7a", 10949,
		"1ef11db1-7b06-44ec-b5d2-af3a4b9a249f",
		priorityDefault, time.Unix(1002, 0))

	// Enqueue an operation after the previous two as later invocation, still default priority.
	enqueueOperation(t, ctrl, ctxEarly, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"12039d6dd9a7e27622301e935b6eefc78846802e", 11,
		"163d8e5e390b106c99e9a23fe5a1058337ad1930", 22,
		"d3ecfdda-b3ad-11ea-b3de-0242ac130004",
		priorityDefault, time.Unix(1003, 0))

	// Enqueue another after the previous ones as later invocation, still default priority.
	enqueueOperation(t, ctrl, ctxMedium, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"4fbfcfddd17e0aa37e30ab076acdb58e3bf6bb1f", 33,
		"e8eee4d468b6fe6e915330a4c3882f9fb5990a8d", 44,
		"032eaa58-b3ae-11ea-b3de-0242ac130004",
		priorityDefault, time.Unix(1004, 0))

	// Enqueue another operation of the early invocation, but low priority, so it will be put last.
	enqueueOperation(t, ctrl, ctxEarly, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"2f9f0f479db29c552df5ff190a683200caa9d569", 55,
		"2c9537d27c201a226df5ca2d3babf2515f9600d7", 66,
		"102a980c-b3ae-11ea-b3de-0242ac130004",
		priorityLow, time.Unix(1005, 0))

	// Enqueue a new invocation, but with high priority, so it appears first.
	enqueueOperation(t, ctrl, ctxLate, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"2f4458df6c47896395928b74158a6d9d9791bda0", 77,
		"9b1df8e00f274c5ff430d0c02d06ceabfe0dbb3a", 88,
		"1a77fe58-b3ae-11ea-b3de-0242ac130004",
		priorityHigh, time.Unix(1006, 0))

	// Enqueue another default priority operation, now veary early invocation, so 3rd order in default priority.
	enqueueOperation(t, ctrl, ctxVeryEarly, contentAddressableStorage, executionClient, uuidGenerator, clock, platform, buildQueue,
		"e2cba501f5a59db48cfef64e017a3b91916c4303", 99,
		"962d944b0d0b93b7c8231e9926c28fdff7dd3617", 110,
		"250deff8-b3ae-11ea-b3de-0242ac130004",
		priorityDefault, time.Unix(1007, 0))

	return buildQueue, clock, platform
}

// Do enqueue an operation with the specified action(hash) and command(hash)
// which will have the specified time as queued time.
func enqueueOperation(t *testing.T,
	ctrl *gomock.Controller,
	ctx context.Context,
	contentAddressableStorage *mock.MockBlobAccess,
	executionClient remoteexecution.ExecutionClient,
	uuidGenerator *mock.MockUUIDGenerator,
	clock *mock.MockClock,
	platform *remoteexecution.Platform,
	buildQueue *re_builder.InMemoryBuildQueue,
	actionHash string,
	actionHashSize int64,
	commandHash string,
	commandHashSize int64,
	anUUID string,
	priority int32,
	clockTime time.Time) {

	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest(INSTANCE_NAME, actionHash, actionHashSize),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      commandHash,
			SizeBytes: commandHashSize,
		},
	}, buffer.UserProvided))
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest(INSTANCE_NAME, commandHash, commandHashSize),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Platform: platform,
	}, buffer.UserProvided))
	uuidGenerator.EXPECT().Call().Return(uuid.Parse(anUUID))
	clock.EXPECT().Now().Return(clockTime)
	timer := mock.NewMockTimer(ctrl)
	wakeup := make(chan time.Time, 1)
	clock.EXPECT().NewTimer(time.Minute).Return(timer, wakeup)
	var executionPolicy *remoteexecution.ExecutionPolicy
	if priority == int32(0) {
		executionPolicy = &remoteexecution.ExecutionPolicy{}
	} else {
		executionPolicy = &remoteexecution.ExecutionPolicy{
			Priority: priority,
		}
	}
	stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: INSTANCE_NAME,
		ActionDigest: &remoteexecution.Digest{
			Hash:      actionHash,
			SizeBytes: actionHashSize,
		},
		ExecutionPolicy: executionPolicy,
	})
	require.NoError(t, err)
	update, err := stream.Recv()
	require.NoError(t, err)
	metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecutionStage_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      actionHash,
			SizeBytes: actionHashSize,
		},
	})
	require.NoError(t, err)
	require.Equal(t, update, &longrunning.Operation{
		Name:     anUUID,
		Metadata: metadata,
	})
}

// TODO: Make testing coverage of InMemoryBuildQueue complete.
