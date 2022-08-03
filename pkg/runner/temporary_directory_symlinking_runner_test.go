package runner_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTemporaryDirectorySymlinkingRunnerRun(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve("/worker/build", scopeWalker))

	t.Run("InvalidTemporaryDirectory", func(t *testing.T) {
		// The temporary directory path provided by bb_worker is
		// invalid. This should cause the symbolic link creation
		// to fail.
		baseRunner := mock.NewMockRunnerServer(ctrl)
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, "/hello", buildDirectory)

		_, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments:          []string{"cc", "-o", "hello.o", "hello.c"},
			WorkingDirectory:   "a/root/subdir",
			StdoutPath:         "a/stdout",
			StderrPath:         "a/stderr",
			InputRootDirectory: "a/root",
			TemporaryDirectory: "a/\x00tmp",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve temporary directory: Path contains a null byte"), err)
	})

	t.Run("InvalidSymlinkPath", func(t *testing.T) {
		// Failures to replace the provided path with a symbolic
		// link should be propagated.
		baseRunner := mock.NewMockRunnerServer(ctrl)
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, "/", buildDirectory)

		_, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments:          []string{"cc", "-o", "hello.o", "hello.c"},
			WorkingDirectory:   "a/root/subdir",
			StdoutPath:         "a/stdout",
			StderrPath:         "a/stderr",
			InputRootDirectory: "a/root",
			TemporaryDirectory: "a/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to remove symbolic link \"/\": "), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully replace the provided path with a
		// symbolic link. The execution request should be
		// forwarded to the underlying Runner. The symbolic link
		// should have the right contents.
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

		baseRunner := mock.NewMockRunnerServer(ctrl)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).Return(response, nil)
		symlinkPath := filepath.Join(t.TempDir(), "symlink")
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, symlinkPath, buildDirectory)

		observedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)

		symlinkTarget, err := os.Readlink(symlinkPath)
		require.NoError(t, err)
		require.Equal(t, filepath.FromSlash("/worker/build/a/tmp"), symlinkTarget)
	})
}

func TestTemporaryDirectorySymlinkingRunnerCheckReadiness(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve("/worker/build", scopeWalker))

	t.Run("InvalidSymlinkPath", func(t *testing.T) {
		// Readiness checks should fail in case the path at
		// which the symlink needs to be stored is invalid.
		baseRunner := mock.NewMockRunnerServer(ctrl)
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, "/", buildDirectory)

		_, err := runner.CheckReadiness(ctx, &emptypb.Empty{})
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to remove symbolic link \"/\": "), err)
	})

	t.Run("NoopWhenBusy", func(t *testing.T) {
		// The readiness check should be a no-op in case there
		// are one or more actions running. We don't want to
		// change the symbolic link to point to a location for
		// testing, as that would interfere with the action.
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

		baseRunner := mock.NewMockRunnerServer(ctrl)
		symlinkPath := filepath.Join(t.TempDir(), "symlink")
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, symlinkPath, buildDirectory)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).DoAndReturn(
			func(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
				// At the start of the action, the
				// symbolic link should point to the
				// temporary directory provided by the
				// worker.
				symlinkTarget, err := os.Readlink(symlinkPath)
				require.NoError(t, err)
				require.Equal(t, filepath.FromSlash("/worker/build/a/tmp"), symlinkTarget)

				// Concurrent readiness check calls
				// should still be forwarded.
				baseRunner.EXPECT().CheckReadiness(ctx, testutil.EqProto(t, &emptypb.Empty{})).Return(&emptypb.Empty{}, nil)

				_, err = runner.CheckReadiness(ctx, &emptypb.Empty{})
				require.NoError(t, err)

				// The symlink should not get altered in
				// the meantime.
				symlinkTarget, err = os.Readlink(symlinkPath)
				require.NoError(t, err)
				require.Equal(t, filepath.FromSlash("/worker/build/a/tmp"), symlinkTarget)
				return response, nil
			})

		observedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})

	t.Run("Success", func(t *testing.T) {
		// In case no actions are running, readiness checks
		// should cause the symbolic link to be created for
		// testing purposes.
		baseRunner := mock.NewMockRunnerServer(ctrl)
		baseRunner.EXPECT().CheckReadiness(ctx, testutil.EqProto(t, &emptypb.Empty{})).Return(&emptypb.Empty{}, nil)
		symlinkPath := filepath.Join(t.TempDir(), "symlink")
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, symlinkPath, buildDirectory)

		_, err := runner.CheckReadiness(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		symlinkTarget, err := os.Readlink(symlinkPath)
		require.NoError(t, err)
		require.Equal(t, filepath.FromSlash("/nonexistent"), symlinkTarget)
	})
}
