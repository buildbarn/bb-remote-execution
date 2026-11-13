package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
)

func TestPrefetchingBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	fileReadSemaphore := semaphore.NewWeighted(1)
	fileSystemAccessCache := mock.NewMockBlobAccess[*fsac.FileSystemAccessProfile](ctrl)
	buildExecutor := builder.NewPrefetchingBuildExecutor(
		baseBuildExecutor,
		cas.ReadInfo{
			ChunkMappingFetcher:  chunkMappingFetcher,
			CDCParametersFetcher: cdcParametersFetcher,
		},
		chunkStorage,
		directoryFetcher,
		fileReadSemaphore,
		fileSystemAccessCache,
		/* bloomFilterBitsPerElement = */ 10,
		/* bloomFilterMaximumSizeBytes = */ 1000,
		/* logFileSystemAccessProfile = */ false,
		mock.NewMockProtoPutter(ctrl),
	)

	filePool := mock.NewMockFilePool(ctrl)
	baseMonitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)
	executionStateUpdates := make(chan<- *remoteworker.CurrentState_Executing)

	// Use the minimum chunk size, so that tests can distinguish
	// single-chunk and multi-chunk files using small blobs.
	cdcParametersFetcher.EXPECT().
		FetchCDCParameters(gomock.Any(), gomock.Any()).
		Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}, nil).
		AnyTimes()

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
				executionStateUpdates,
			),
		)
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
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) (*fsac.FileSystemAccessProfile, error) {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				return nil, status.Error(codes.Canceled, "Request cancelled")
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
				executionStateUpdates,
			),
		)
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
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) (*fsac.FileSystemAccessProfile, error) {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				return nil, status.Error(codes.Canceled, "Request cancelled")
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
				executionStateUpdates,
			),
		)
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
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			Return(nil, status.Error(codes.Internal, "Storage offline"))

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
				executionStateUpdates,
			),
		)
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
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, nil)
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
				executionStateUpdates,
			),
		)
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
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, nil)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "095afbd64b6358665546558583c64d38", 456)).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "hello.txt",
						Digest: &remoteexecution.Digest{
							Hash:      "8b1a9953c4611296a827abf8c47804d7",
							SizeBytes: 5,
						},
					},
				},
			}, nil)
		chunkStorage.EXPECT().Prefetch(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet()).
			Return(status.Error(codes.Internal, "Storage offline"))

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
				executionStateUpdates,
			),
		)
	})

	t.Run("ContentAddressableStorageMissing", func(t *testing.T) {
		// If a file turns out to be missing from the CAS while
		// prefetching, it should be reported as a
		// FAILED_PRECONDITION error that refers to the file, not to
		// one of its chunks.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, nil)
		fileDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "095afbd64b6358665546558583c64d38", 456)).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:   "hello.txt",
						Digest: fileDigest.GetProto(),
					},
				},
			}, nil)
		chunkStorage.EXPECT().Prefetch(gomock.Any(), fileDigest.ToSingletonSet()).
			Return(status.Error(codes.NotFound, "Blob not found"))

		wantStatus, err := status.New(codes.FailedPrecondition, "Failed to prefetch file \"hello.txt\": Blob not found").WithDetails(
			&errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{
						Type: "MISSING",
						Subject: digest.NewInstanceNamePatcher(fileDigest.GetInstanceName(), digest.EmptyInstanceName).
							PatchDigest(fileDigest).
							GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
					},
				},
			},
		)
		require.NoError(t, err)

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: wantStatus.Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
	})

	t.Run("ContentAddressableStorageMissingChunked", func(t *testing.T) {
		// For a file that is made up of multiple chunks, a missing
		// chunk must still be reported as a FAILED_PRECONDITION
		// error that refers to the file, not to the chunk. This is
		// the case that regressed when the existence precondition
		// was applied at the chunk level.
		baseBuildExecutor.EXPECT().Execute(
			gomock.Any(),
			filePool,
			gomock.Any(),
			digestFunction,
			testutil.EqProto(t, exampleRequest),
			executionStateUpdates,
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0xff},
				BloomFilterHashFunctions: 1,
			}, nil)
		// A file of 143 bytes is larger than twice the minimum
		// chunk size, so it is stored as two chunks.
		fileDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "c1df2b2b8aa945f29de62076a8f1e2d9", 143)
		chunk1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "9871f053ed93e778d5090e3dc038815d", 77)
		chunk2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "2044d462a0be5850ee579b02a1ca3b25", 66)
		mapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1, chunk2}, 143, true)
		require.NoError(t, err)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "095afbd64b6358665546558583c64d38", 456)).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:   "hello.txt",
						Digest: fileDigest.GetProto(),
					},
				},
			}, nil)
		chunkMappingFetcher.EXPECT().FetchChunkMapping(gomock.Any(), fileDigest).Return(mapping, nil)
		chunkStorage.EXPECT().Prefetch(gomock.Any(), digest.GetUnion([]digest.Set{chunk1.ToSingletonSet(), chunk2.ToSingletonSet()})).
			Return(status.Error(codes.NotFound, "Chunk not found"))

		wantStatus, err := status.New(codes.FailedPrecondition, "Failed to prefetch file \"hello.txt\": Chunk not found").WithDetails(
			&errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{
						Type: "MISSING",
						Subject: digest.NewInstanceNamePatcher(fileDigest.GetInstanceName(), digest.EmptyInstanceName).
							PatchDigest(fileDigest).
							GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
					},
				},
			},
		)
		require.NoError(t, err)

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: wantStatus.Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x00, 0x20},
				BloomFilterHashFunctions: 1,
			}, nil)
		fileSystemAccessCache.EXPECT().Put(gomock.Any(), exampleReducedActionDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, profile *fsac.FileSystemAccessProfile) error {
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
				Status: status.New(codes.Internal, "Failed to store file system access profile to FSAC: Storage offline").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
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
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, nil)

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
				executionStateUpdates,
			),
		)
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
		).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			monitor.ReadDirectory()
			return &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
			}
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, nil)
		fileSystemAccessCache.EXPECT().Put(gomock.Any(), exampleReducedActionDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, profile *fsac.FileSystemAccessProfile) error {
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
				executionStateUpdates,
			),
		)
	})

	// The build executor that will be used in the tests below
	// to test the log file system access profile configuration.
	protoPutter := mock.NewMockProtoPutter(ctrl)
	profileLoggingBuildExecutor := builder.NewPrefetchingBuildExecutor(
		baseBuildExecutor,
		cas.ReadInfo{
			ChunkMappingFetcher:  chunkMappingFetcher,
			CDCParametersFetcher: cdcParametersFetcher,
		},
		chunkStorage,
		directoryFetcher,
		fileReadSemaphore,
		fileSystemAccessCache,
		/* bloomFilterBitsPerElement = */ 10,
		/* bloomFilterMaximumSizeBytes = */ 1000,
		/* logFileSystemAccessProfile = */ true,
		protoPutter,
	)

	t.Run("CASLogAccessProfileSuccess", func(t *testing.T) {
		// Successfully store file system access profile in
		// Content Addressable Storage (CAS) and attach the
		// profile digest to the response server logs.
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
			ServerLogs: nil, // Responses are not guaranteed to already contain any server logs.
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, nil)
		accessProfileDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "dc1d3a7d5a534e6175cdc57c96cc031d", 5)
		protoPutter.EXPECT().PutProto(
			gomock.Any(),
			testutil.EqProto(t, &fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}),
			gomock.Any(),
		).Return(accessProfileDigest, nil)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				ServerLogs: map[string]*remoteexecution.LogFile{
					"buildbarn.fsac.FileSystemAccessProfile": {
						Digest:        accessProfileDigest.GetProto(),
						HumanReadable: false,
					},
				},
			},
			profileLoggingBuildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
	})

	t.Run("CASLogAccessProfileError", func(t *testing.T) {
		// If storing the file system access profile to Content
		// Addressable Storage (CAS) fails the error should be
		// propagated and the profile digest should not be
		// attached to the response server logs.
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
			ServerLogs: nil, // Responses are not guaranteed to already contain any server logs.
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, nil)
		protoPutter.EXPECT().PutProto(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(digest.BadDigest, status.Error(codes.Internal, "Storage offline"))

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				Status: status.New(codes.Internal, "Failed to store file system access profile to CAS: Storage offline").Proto(),
			},
			profileLoggingBuildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
	})

	t.Run("DoNotOverwriteServerLogs", func(t *testing.T) {
		// Response server logs must retain preexisting data.
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
			ServerLogs: map[string]*remoteexecution.LogFile{
				"existing-log": {},
			},
		})
		fileSystemAccessCache.EXPECT().Get(gomock.Any(), exampleReducedActionDigest).
			Return(&fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}, nil)
		accessProfileDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "dc1d3a7d5a534e6175cdc57c96cc031d", 5)
		protoPutter.EXPECT().PutProto(
			gomock.Any(),
			testutil.EqProto(t, &fsac.FileSystemAccessProfile{
				BloomFilter:              []byte{0x80},
				BloomFilterHashFunctions: 1,
			}),
			gomock.Any(),
		).Return(accessProfileDigest, nil)

		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{defaultInputRootResourceUsage},
					},
				},
				ServerLogs: map[string]*remoteexecution.LogFile{
					"existing-log": {},
					"buildbarn.fsac.FileSystemAccessProfile": {
						Digest:        accessProfileDigest.GetProto(),
						HumanReadable: false,
					},
				},
			},
			profileLoggingBuildExecutor.Execute(
				ctx,
				filePool,
				baseMonitor,
				digestFunction,
				exampleRequest,
				executionStateUpdates,
			),
		)
	})
}
