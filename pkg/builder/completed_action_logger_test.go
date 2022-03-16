package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	mock "github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemoteCompletedActionLogger(t *testing.T) {
	ctrl, _ := gomock.WithContext(context.Background(), t)
	conn := mock.NewMockClientConnInterface(ctrl)
	client := cal_proto.NewCompletedActionLoggerClient(conn)
	logger := builder.NewRemoteCompletedActionLogger(100, client)

	completedAction := &cal_proto.CompletedAction{
		HistoricalExecuteResponse: &cas_proto.HistoricalExecuteResponse{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			},
			ExecuteResponse: &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						WorkerStartTimestamp:     &timestamppb.Timestamp{Nanos: 0},
						WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 500000000},
						AuxiliaryMetadata:        []*anypb.Any{},
					},
				},
			},
		},
		Uuid:         "uuid",
		InstanceName: "freebsd12",
	}

	t.Run("OnlyRecvCallsFailure", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		conn.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/buildbarn.completedactionlogger.CompletedActionLogger/LogCompletedActions", gomock.Any()).
			DoAndReturn(func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return clientStream, nil
			})

		clientStream.EXPECT().RecvMsg(gomock.Any())
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))
		err := logger.SendAllCompletedActions()
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Improper response: No messages left in the queue"), err)
	})

	t.Run("DroppedConnectionRetrySuccess", func(t *testing.T) {
		var savedCtx1 context.Context
		clientStream1 := mock.NewMockClientStream(ctrl)
		conn.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/buildbarn.completedactionlogger.CompletedActionLogger/LogCompletedActions", gomock.Any()).
			DoAndReturn(func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				savedCtx1 = ctx
				return clientStream1, nil
			})

		for i := 0; i < 5; i++ {
			ch1 := make(chan struct{})
			clientStream1.EXPECT().SendMsg(completedAction).DoAndReturn(func(i interface{}) error {
				close(ch1)
				return nil
			})
			clientStream1.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(i interface{}) error {
				<-ch1
				return nil
			})
		}
		clientStream1.EXPECT().SendMsg(completedAction).DoAndReturn(func(i interface{}) error {
			return nil
		}).MaxTimes(5)
		clientStream1.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(i interface{}) error {
			return status.Error(codes.Unavailable, "Server on fire")
		})
		clientStream1.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			<-savedCtx1.Done()
			require.Equal(t, context.Canceled, savedCtx1.Err())
			return status.Error(codes.Canceled, "Request canceled")
		})

		for i := 0; i < 10; i++ {
			logger.LogCompletedAction(completedAction)
		}
		// Start the client so that it may transmit the 10 queued messages.
		err := logger.SendAllCompletedActions()
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to receive response from server: Server on fire"), err)

		// We expect five more items in the logger's sendQueue at
		// this point. Perform the setup to start the logger queue
		// again and resend the last message.
		var savedCtx2 context.Context
		clientStream2 := mock.NewMockClientStream(ctrl)
		conn.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/buildbarn.completedactionlogger.CompletedActionLogger/LogCompletedActions", gomock.Any()).
			DoAndReturn(func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				savedCtx2 = ctx
				return clientStream2, nil
			})

		// Successfully retry the five remaining messages left in the queue.
		for i := 0; i < 5; i++ {
			ch3 := make(chan struct{})
			clientStream2.EXPECT().SendMsg(completedAction).DoAndReturn(func(i interface{}) error {
				close(ch3)
				return nil
			})
			clientStream2.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(i interface{}) error {
				<-ch3
				return nil
			})
		}
		// All messages have been sent, now force an exit by
		// returning an error on subsequent RecvMsg calls.
		clientStream2.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(i interface{}) error {
			return status.Error(codes.Unavailable, "Server out of memory")
		})
		clientStream2.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			<-savedCtx2.Done()
			require.Equal(t, context.Canceled, savedCtx2.Err())
			return status.Error(codes.Canceled, "Request canceled")
		})
		err = logger.SendAllCompletedActions()
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to receive response from server: Server out of memory"), err)
	})
}
