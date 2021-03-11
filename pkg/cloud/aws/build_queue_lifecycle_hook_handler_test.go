package aws_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestBuildQueueLifecycleHookHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildQueue := mock.NewMockBuildQueueStateServer(ctrl)
	lhh := re_aws.NewBuildQueueLifecycleHookHandler(buildQueue, "instance")

	t.Run("Succes", func(t *testing.T) {
		buildQueue.EXPECT().TerminateWorkers(
			context.Background(),
			&buildqueuestate.TerminateWorkersRequest{
				WorkerIdPattern: map[string]string{
					"instance": "i-59438147357578398",
				},
			}).Return(&emptypb.Empty{}, nil)

		require.NoError(t, lhh.HandleEC2InstanceTerminating("i-59438147357578398"))
	})

	t.Run("Failure", func(t *testing.T) {
		buildQueue.EXPECT().TerminateWorkers(
			context.Background(),
			&buildqueuestate.TerminateWorkersRequest{
				WorkerIdPattern: map[string]string{
					"instance": "i-59438147357578398",
				},
			}).Return(nil, status.Error(codes.Internal, "Server on fire"))

		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			lhh.HandleEC2InstanceTerminating("i-59438147357578398"))
	})
}
