package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestStorageFlushingBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	storageFlusher := mock.NewMockStorageFlusher(ctrl)
	buildExecutor := builder.NewStorageFlushingBuildExecutor(baseBuildExecutor, storageFlusher.Call)

	// Execute request and response that are used for all tests. The
	// response uses all features supported by the protocol, to test
	// that we only strip fields that ought to be omitted.
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
		Action: &remoteexecution.Action{DoNotCache: false},
	}
	updates := make(chan<- *remoteworker.CurrentState_Executing)
	response := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "output.o",
					Digest: &remoteexecution.Digest{
						Hash:      "8c2e88f122b6fbcf0a20d562391c93db",
						SizeBytes: 3483,
					},
				},
			},
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "some_directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "0342e9502cf8c4cea71de4c33669b60f",
						SizeBytes: 237944,
					},
				},
			},
			OutputSymlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "output.o.stripped",
					Target: "output.o",
				},
				{
					Path:   "some_other_directory",
					Target: "some_directory",
				},
			},
			ExitCode:  123,
			StdoutRaw: []byte("Hello"),
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 5,
			},
			StderrRaw: []byte("Hello"),
			StderrDigest: &remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 5,
			},
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				Worker: "builder1.example.com",
			},
		},
		ServerLogs: map[string]*remoteexecution.LogFile{
			"kernel_log": {
				Digest: &remoteexecution.Digest{
					Hash:      "2917c2a7eb23012392098e74a873cd31",
					SizeBytes: 9584,
				},
				HumanReadable: true,
			},
		},
		Message: "Uncached action result: http://....",
	}

	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("default", remoteexecution.DigestFunction_MD5)
	baseBuildExecutor.EXPECT().Execute(
		ctx, filePool, monitor, digestFunction, request, updates,
	).Return(proto.Clone(response).(*remoteexecution.ExecuteResponse)).Times(2)

	// When flushing succeeds, we should return the response in
	// literal form.
	t.Run("FlushingSucceeded", func(t *testing.T) {
		storageFlusher.EXPECT().Call(ctx).Return(nil)
		testutil.RequireEqualProto(
			t,
			response,
			buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, updates))
	})

	// When flushing fails, some of the outputs may not have ended
	// up in storage. Return the response with all of the digests
	// removed.
	t.Run("FlushingFailed", func(t *testing.T) {
		storageFlusher.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Failed to flush blobs to storage"))
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					OutputSymlinks: []*remoteexecution.OutputSymlink{
						{
							Path:   "output.o.stripped",
							Target: "output.o",
						},
						{
							Path:   "some_other_directory",
							Target: "some_directory",
						},
					},
					ExitCode:  123,
					StdoutRaw: []byte("Hello"),
					StderrRaw: []byte("Hello"),
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						Worker: "builder1.example.com",
					},
				},
				Status:  status.New(codes.Internal, "Failed to flush blobs to storage").Proto(),
				Message: "Uncached action result: http://....",
			},
			buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, updates))
	})
}
