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
)

func TestTemporaryDirectorySymlinkingRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve("/worker/build", scopeWalker))

	t.Run("InvalidTemporaryDirectory", func(t *testing.T) {
		// The temporary directory path provided by bb_worker is
		// invalid. This should cause the symbolic link creation
		// to fail.
		baseRunner := mock.NewMockRunner(ctrl)
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, "/hello", buildDirectory)

		_, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments:          []string{"cc", "-o", "hello.o", "hello.c"},
			WorkingDirectory:   "a/root/subdir",
			StdoutPath:         "a/stdout",
			StderrPath:         "a/stderr",
			InputRootDirectory: "a/root",
			TemporaryDirectory: "a/\x00tmp",
		})
		require.Equal(t, status.Error(codes.InvalidArgument, "Failed to resolve temporary directory: Path contains a null byte"), err)
	})

	t.Run("InvalidSymlinkPath", func(t *testing.T) {
		// Failures to replace the provided path with a symbolic
		// link should be propagated.
		baseRunner := mock.NewMockRunner(ctrl)
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

		baseRunner := mock.NewMockRunner(ctrl)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).Return(response, nil)
		symlinkPath := filepath.Join(t.TempDir(), "symlink")
		runner := runner.NewTemporaryDirectorySymlinkingRunner(baseRunner, symlinkPath, buildDirectory)

		observedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)

		observedSymlinkPath, err := os.Readlink(symlinkPath)
		require.NoError(t, err)
		require.Equal(t, "/worker/build/a/tmp", observedSymlinkPath)
	})
}
