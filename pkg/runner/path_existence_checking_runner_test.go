package runner_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestPathExistenceCheckingRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	mockRunner := mock.NewMockRunnerServer(ctrl)
	readinessCheckingFilename := filepath.Join(t.TempDir(), "ready")
	runnerServer := runner.NewPathExistenceCheckingRunner(mockRunner, []string{
		readinessCheckingFilename,
	})

	runRequest := &runner_pb.RunRequest{
		Arguments: []string{"ls", "-l"},
	}
	runResponse := &runner_pb.RunResponse{
		ExitCode: 42,
	}

	t.Run("NotReadyCheckReadiness", func(t *testing.T) {
		// When the file used for readiness checking is not
		// present, CheckReadiness() should fail.
		_, err := runnerServer.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
		testutil.RequirePrefixedStatus(
			t,
			status.Errorf(codes.Unavailable, "Path %#v: ", readinessCheckingFilename),
			err)
	})

	t.Run("NotReadyRun", func(t *testing.T) {
		// Similarly, the results of failing Run() calls should
		// be suppressed, so that build failures are prevented.
		mockRunner.EXPECT().Run(ctx, runRequest).Return(&runner_pb.RunResponse{
			ExitCode: 42,
		}, nil)

		_, err := runnerServer.Run(ctx, runRequest)
		testutil.RequirePrefixedStatus(
			t,
			status.Errorf(codes.Unavailable, "One or more required files disappeared during execution: Path %#v: ", readinessCheckingFilename),
			err)
	})

	// Create the file used for readiness checking and repeat the
	// tests above.
	f, err := os.OpenFile(readinessCheckingFilename, os.O_CREATE|os.O_WRONLY, 0o666)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Run("ReadyCheckReadiness", func(t *testing.T) {
		// Readiness checks should now succeed.
		mockRunner.EXPECT().CheckReadiness(ctx, gomock.Any()).Return(&emptypb.Empty{}, nil)

		_, err := runnerServer.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
		require.NoError(t, err)
	})

	t.Run("ReadyRun", func(t *testing.T) {
		// If readiness checks pass, non-zero exit codes of
		// build actions should be returned as is.
		mockRunner.EXPECT().Run(ctx, runRequest).Return(runResponse, nil)

		observedRunResponse, err := runnerServer.Run(ctx, runRequest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, runResponse, observedRunResponse)
	})
}
