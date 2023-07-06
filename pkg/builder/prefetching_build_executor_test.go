package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPrefetchingBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	fileReadSemaphore := semaphore.NewWeighted(1)
	fileSystemAccessCache := mock.NewMockBlobAccess(ctrl)
	buildExecutor := builder.NewPrefetchingBuildExecutor(
		baseBuildExecutor,
		contentAddressableStorage,
		directoryFetcher,
		fileReadSemaphore,
		fileSystemAccessCache,
		/* maximumMessageSizeBytes = */ 10000,
		/* bloomFilterBitsPerElement = */ 10,
		/* bloomFilterMaximumSizeBytes = */ 1000)

	filePool := mock.NewMockFilePool(ctrl)
	baseMonitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)
	executionStateUpdates := make(chan<- *remoteworker.CurrentState_Executing)

	defaultInputRootResourceUsage, err := anypb.New(&resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 1,
		DirectoriesRead:     0,
		FilesRead:           0,
	})
	require.NoError(t, err)

	t.Run("ActionMissing", func(t *testing.T) {
		// If no Action is present, there is no way to compute
		// the reduced Action digest that's needed to fetch the
		// profile.
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Request does not contain an action").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				&remoteworker.DesiredState_Executing{},
				executionStateUpdates))
	})

	// The request that will be used in the tests below, and the
	// reduced action digest that corresponds with it.
	exampleRequest := &remoteworker.DesiredState_Executing{
		Action: &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "e72499b7c6de12b7ad046541f6de8beb",
				SizeBytes: 123,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "095afbd64b6358665546558583c64d38",
				SizeBytes: 456,
			},
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "os", Value: "linux"},
				},
			},
		},
	}
	exampleReducedActionDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8f4c066b7911c44acae9e11f42889828", 53)

	t.Run("NonZeroExitCode", func(t *testing.T) {
		// If the action fails with a non-zero exit code while
		// we're still trying to load the profile, there's no
		// point in completing the request. We can return
		// immediately.
		response := &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				ExitCode:          12,
			},
		}
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).Return(response)
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				return buffer.NewBufferFromError(status.Error(codes.Canceled, "Request cancelled"))
			})

		testutil.RequireEqualProto(
			t,
			response,
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("ExecutionFailure", func(t *testing.T) {
		// The same holds if execution fails due to some kind of
		// infrastructure issue.
		response := &remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			},
			Status: status.New(codes.Internal, "Cannot fork child process").Proto(),
		}
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).Return(response)
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				return buffer.NewBufferFromError(status.Error(codes.Canceled, "Request cancelled"))
			})

		testutil.RequireEqualProto(
			t,
			response,
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("FSACGetError", func(t *testing.T) {
		// Errors reading from the File System Access Cache
		// (FSAC) should be propagated. It should cause the
		// execution of the action to be cancelled immediately.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			<-ctx.Done()
			require.Equal(t, context.Canceled, ctx.Err())
			return &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.Canceled, "Execution cancelled").Proto(),
			}
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Storage offline")))

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: status.New(codes.Internal, "Failed to fetch file system access profile: Storage offline").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("DirectoryFetcherGetError", func(t *testing.T) {
		// Errors fetching directories from the Content
		// Addressable Storage (CAS) should be propagated.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			<-ctx.Done()
			require.Equal(t, context.Canceled, ctx.Err())
			return &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.Canceled, "Execution cancelled").Proto(),
			}
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewProtoBufferFromProto(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, buffer.UserProvided))
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "095afbd64b6358665546558583c64d38", 456)).
			Return(nil, status.Error(codes.Internal, "Storage offline"))

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: status.New(codes.Internal, "Failed to prefetch directory \".\": Storage offline").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("ContentAddressableStorageGetError", func(t *testing.T) {
		// Similarly, errors fetching files from the Content
		// Addressable Storage (CAS) should be propagated.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			<-ctx.Done()
			require.Equal(t, context.Canceled, ctx.Err())
			return &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.Canceled, "Execution cancelled").Proto(),
			}
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewProtoBufferFromProto(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, buffer.UserProvided))
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "095afbd64b6358665546558583c64d38", 456)).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "hello.txt",
						Digest: &remoteexecution.Digest{
							Hash:      "3ffe1ce0624ece24e5d9b31c2342a6d4",
							SizeBytes: 200,
						},
					},
				},
			}, nil)
		contentAddressableStorage.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3ffe1ce0624ece24e5d9b31c2342a6d4", 200)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Storage offline")))

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: status.New(codes.Internal, "Failed to prefetch file \"hello.txt\": Storage offline").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("FSACPutError", func(t *testing.T) {
		// If the Bloom filter stored in the profile does not
		// match with the paths accessed during execution, we
		// should attempt to write an updated profile. Failures
		// writing this profile should be propagated.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).Return(&remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			},
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewProtoBufferFromProto(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x00, 0x20},
				BloomFilterHashFunctions: 1,
			}, buffer.UserProvided))
		fileSystemAccessCache.EXPECT().Put(gomock.Any(), exampleReducedActionDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Storage offline")
			})

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: status.New(codes.Internal, "Failed to store file system access profile: Storage offline").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("FSACPutSkipped", func(t *testing.T) {
		// If the profile matches was read from the FSAC, then
		// there is no need to store an updated profile.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).Return(&remoteexecution.ExecuteResponse{
			Result: &remoteexecution.ActionResult{
				ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			},
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewProtoBufferFromProto(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, buffer.UserProvided))

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})

	t.Run("FSACPutSuccess", func(t *testing.T) {
		// Successfully overwrite a Bloom filter that was out of
		// sync with what's observed while executing.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			monitor.ReadDirectory()
			return &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
			}
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(buffer.NewProtoBufferFromProto(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, buffer.UserProvided))
		fileSystemAccessCache.EXPECT().Put(gomock.Any(), exampleReducedActionDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				profile, err := b.ToProto(&fsac.FileSystemAccessProfile{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &fsac.FileSystemAccessProfile{
					BloomFilter:              []byte{0x0b, 0x2a},
					BloomFilterHashFunctions: 9,
				}, profile)
				return nil
			})

		inputRootResourceUsage, err := anypb.New(&resourceusage.InputRootResourceUsage{
			DirectoriesResolved: 1,
			DirectoriesRead:     1,
			FilesRead:           0,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{inputRootResourceUsage},
					},
				},
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates))
	})
}
