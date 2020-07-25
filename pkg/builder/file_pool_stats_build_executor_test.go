package builder_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/require"
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
	baseBuildExecutor.EXPECT().Execute(
		ctx,
		gomock.Any(),
		digest.MustNewInstanceName("hello"),
		request,
		gomock.Any()).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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

	// Perform the execution request.
	executionStateUpdates := make(chan *remoteworker.CurrentState_Executing, 3)
	buildExecutor := builder.NewFilePoolStatsBuildExecutor(baseBuildExecutor)
	executeResponse := buildExecutor.Execute(
		ctx,
		filesystem.NewInMemoryFilePool(),
		digest.MustNewInstanceName("hello"),
		request,
		executionStateUpdates)

	// Validate the execute response, which should now contain the
	// file pool resource usage statistics.
	resourceUsage, err := ptypes.MarshalAny(&resourceusage.FilePoolResourceUsage{
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
	require.True(t, proto.Equal(executeResponse, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode: 1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: []*any.Any{resourceUsage},
			},
		},
	}))
}
