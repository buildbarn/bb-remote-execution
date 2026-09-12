package runner_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestPathExistenceCheckingPersistentRunner(test *testing.T) {
	base := mock.NewMockPersistentRunnerServer(gomock.NewController(test))
	pathname := filepath.Join(test.TempDir(), "required")
	require.NoError(test, os.WriteFile(pathname, nil, 0o600))
	server := runner.NewPathExistenceCheckingPersistentRunner(base, []string{pathname})
	ctx := context.Background()
	base.EXPECT().CheckReadiness(ctx, gomock.Any()).Return(&emptypb.Empty{}, nil)
	_, err := server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
	require.NoError(test, err)
	base.EXPECT().CreateSession(ctx, gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	_, err = server.CreateSession(ctx, &runner_pb.CreateSessionRequest{})
	require.NoError(test, err)
	response := &runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: []byte{0xff}}
	base.EXPECT().ExecuteInPersistentWorker(ctx, gomock.Any()).Return(response, nil)
	observed, err := server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: "session"})
	require.NoError(test, err)
	require.Same(test, response, observed)
	base.EXPECT().ExecuteInPersistentWorker(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
		require.NoError(test, os.Remove(pathname))
		return response, nil
	})
	_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: "session"})
	require.Error(test, err)
	_, err = server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
	require.Error(test, err)
	_, err = server.CreateSession(ctx, &runner_pb.CreateSessionRequest{})
	require.Error(test, err)
}
