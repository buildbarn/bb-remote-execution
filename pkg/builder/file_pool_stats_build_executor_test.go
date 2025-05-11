package builder_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
)

func TestFilePoolStatsBuildExecutorExample(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Recurring messages used by this test.
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "d41d8cd98f00b204e9800998ecf8427e",
			SizeBytes: 123,
		},
	}

	// Expect to see an execution request. Inside the execution
	// request, generate some I/O on the file pool to produce
	// non-zero counters.
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	baseBuildExecutor.EXPECT().Execute(
		ctx,
		gomock.Any(),
		monitor,
		digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5),
		request,
		gomock.Any()).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
		f, err := filePool.NewFile()
		require.NoError(t, err)
		require.NoError(t, f.Truncate(5))
		require.NoError(t, f.Close())

		f, err = filePool.NewFile()
		require.NoError(t, err)
		n, err := f.WriteAt([]byte("Hello"), 100)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		var p [10]byte
		n, err = f.ReadAt(p[:], 98)
		require.Equal(t, 7, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("\x00\x00Hello\x00\x00\x00"), p[:])
		require.NoError(t, f.Truncate(42))
		require.NoError(t, f.Close())

		return &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExitCode:          1,
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			},
		}
	})

	// Mock all the file operations performed in the execution request.
	filePool := mock.NewMockFilePool(ctrl)
	file1 := mock.NewMockFileReadWriter(ctrl)
	file2 := mock.NewMockFileReadWriter(ctrl)

	filePool.EXPECT().NewFile().Return(file1, nil)
	file1.EXPECT().Truncate(int64(5)).Return(nil)
	file1.EXPECT().Close().Return(nil)
	filePool.EXPECT().NewFile().Return(file2, nil)
	file2.EXPECT().WriteAt([]byte("Hello"), int64(100)).Return(5, nil)
	file2.EXPECT().ReadAt(gomock.Any(), int64(98)).DoAndReturn(func(p []byte, offset int64) (int, error) {
		copy(p, []byte("\x00\x00Hello\x00\x00\x00"))
		return 7, io.EOF
	})
	file2.EXPECT().Truncate(int64(42)).Return(nil)
	file2.EXPECT().Close().Return(nil)

	// Perform the execution request.
	executionStateUpdates := make(chan *remoteworker.CurrentState_Executing, 3)
	buildExecutor := builder.NewFilePoolStatsBuildExecutor(baseBuildExecutor)
	executeResponse := buildExecutor.Execute(
		ctx,
		filePool,
		monitor,
		digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5),
		request,
		executionStateUpdates)

	// Validate the execute response, which should now contain the
	// file pool resource usage statistics.
	resourceUsage, err := anypb.New(&resourceusage.FilePoolResourceUsage{
		FilesCreated:       2,
		FilesCountPeak:     1,
		FilesSizeBytesPeak: 105,
		ReadsCount:         1,
		ReadsSizeBytes:     7,
		WritesCount:        1,
		WritesSizeBytes:    5,
		TruncatesCount:     2,
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode: 1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: []*anypb.Any{resourceUsage},
			},
		},
	}, executeResponse)
}
