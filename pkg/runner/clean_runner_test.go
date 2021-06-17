package runner_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCleanRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseRunner := mock.NewMockRunnerServer(ctrl)
	baseCleaner := mock.NewMockCleaner(ctrl)
	runner := runner.NewCleanRunner(baseRunner, cleaner.NewIdleInvoker(baseCleaner.Call))

	request := &runner_pb.RunRequest{
		Arguments:          []string{"cc", "-o", "hello.o", "hello.c"},
		WorkingDirectory:   "a/root/subdir",
		StdoutPath:         "a/stdout",
		StderrPath:         "a/stderr",
		InputRootDirectory: "a/root",
		TemporaryDirectory: "a/tmp",
	}
	response := &runner_pb.RunResponse{
		ExitCode: 123,
	}

	t.Run("AcquireFailure", func(t *testing.T) {
		// No execution should take place if cleaning doesn't
		// succeed, as the execution environment may not be in a
		// valid state.
		baseCleaner.EXPECT().Call(ctx).
			Return(status.Error(codes.Internal, "Failed to clean temporary directory"))

		_, err := runner.Run(ctx, request)
		require.Equal(t, status.Error(codes.Internal, "Failed to clean temporary directory"), err)
	})

	t.Run("RunFailure", func(t *testing.T) {
		// Execution failures should be propagated. In this case
		// we must still release the IdleInvoker, so that
		// cleanups take place.
		baseCleaner.EXPECT().Call(ctx)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).
			Return(nil, status.Error(codes.InvalidArgument, "\"cc\" not found"))
		baseCleaner.EXPECT().Call(ctx)

		_, err := runner.Run(ctx, request)
		require.Equal(t, status.Error(codes.InvalidArgument, "\"cc\" not found"), err)
	})

	t.Run("ReleaseFailure", func(t *testing.T) {
		// Failures to clean up the execution environment
		// afterwards should also be propagated.
		baseCleaner.EXPECT().Call(ctx)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).
			Return(response, nil)
		baseCleaner.EXPECT().Call(ctx).
			Return(status.Error(codes.Internal, "Failed to clean temporary directory"))

		_, err := runner.Run(ctx, request)
		require.Equal(t, status.Error(codes.Internal, "Failed to clean temporary directory"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseCleaner.EXPECT().Call(ctx)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).
			Return(response, nil)
		baseCleaner.EXPECT().Call(ctx)

		obtainedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, obtainedResponse)
	})
}
