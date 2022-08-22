package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBlobAccessDirectoryFetcherGetDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, 1000)

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Directory object should be propagated.
		directoryDigest := digest.MustNewDigest("example", "756b15c8f94b519e96135dcfde0e58c5", 50)

		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "I/O error"))
		r.EXPECT().Close()
		blobAccess.EXPECT().Get(ctx, directoryDigest).Return(buffer.NewValidatedBufferFromReaderAt(r, 100))

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidDirectory", func(t *testing.T) {
		// It is only valid to call GetDirectory() against an
		// REv2 Directory object.
		directoryDigest := digest.MustNewDigest("example", "764b0da73352b970cfbfc488a0f54934", 30)

		blobAccess.EXPECT().Get(ctx, directoryDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("This is not a Directory object")))

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: "), err)
	})

	t.Run("Success", func(t *testing.T) {
		directoryDigest := digest.MustNewDigest("example", "f5f634611dd11ccba54c7b9d9607c3c2", 100)
		exampleDirectory := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "hello.txt",
					Digest: &remoteexecution.Digest{
						Hash:      "6f6e6ce3fa3aecc5e8275dbfe43a9790",
						SizeBytes: 42,
					},
				},
			},
		}

		blobAccess.EXPECT().Get(ctx, directoryDigest).Return(buffer.NewProtoBufferFromProto(exampleDirectory, buffer.UserProvided))

		directory, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeRootDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, 1000)

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Tree object should be propagated.
		treeDigest := digest.MustNewDigest("example", "756b15c8f94b519e96135dcfde0e58c5", 50)

		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "I/O error"))
		r.EXPECT().Close()
		blobAccess.EXPECT().Get(ctx, treeDigest).Return(buffer.NewValidatedBufferFromReaderAt(r, 100))

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidDirectory", func(t *testing.T) {
		// It is only valid to call GetTreeRootDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", "3478477ca0af085e8d676f9a53b095cb", 25)

		blobAccess.EXPECT().Get(ctx, treeDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("This is not a Tree object")))

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: "), err)
	})

	t.Run("MissingRootDirectory", func(t *testing.T) {
		// Malformed Tree objects may not have a root directory.
		treeDigest := digest.MustNewDigest("example", "f5f634611dd11ccba54c7b9d9607c3c2", 100)

		blobAccess.EXPECT().Get(ctx, treeDigest).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Tree{}, buffer.UserProvided))

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree does not contain a root directory"), err)
	})

	t.Run("Success", func(t *testing.T) {
		treeDigest := digest.MustNewDigest("example", "f5f634611dd11ccba54c7b9d9607c3c2", 100)
		exampleDirectory := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "hello.txt",
					Digest: &remoteexecution.Digest{
						Hash:      "6f6e6ce3fa3aecc5e8275dbfe43a9790",
						SizeBytes: 42,
					},
				},
			},
		}

		blobAccess.EXPECT().Get(ctx, treeDigest).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Tree{
			Root: exampleDirectory,
		}, buffer.UserProvided))

		directory, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeChildDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, 1000)

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Tree object should be propagated.
		treeDigest := digest.MustNewDigest("example", "40d8f0c70941162ee9dfacf8863d23f5", 100)
		directoryDigest := digest.MustNewDigest("example", "756b15c8f94b519e96135dcfde0e58c5", 50)

		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "I/O error"))
		r.EXPECT().Close()
		blobAccess.EXPECT().GetFromComposite(ctx, treeDigest, directoryDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, treeDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
				b, slices := slicer.Slice(buffer.NewValidatedBufferFromReaderAt(r, 100), childDigest)
				require.Empty(t, slices)
				return b
			}).
			AnyTimes()

		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directoryDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidTree", func(t *testing.T) {
		// It is only valid to call GetTreeChildDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", "3478477ca0af085e8d676f9a53b095cb", 25)
		directoryDigest := digest.MustNewDigest("example", "f297d724d679d79d577d46c79fd4d712", 10)

		blobAccess.EXPECT().GetFromComposite(ctx, treeDigest, directoryDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, treeDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
				b, slices := slicer.Slice(buffer.NewValidatedBufferFromByteSlice([]byte("This is not a Tree object")), childDigest)
				require.Empty(t, slices)
				return b
			}).
			AnyTimes()

		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directoryDigest)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: "), err)
	})

	t.Run("ValidTree", func(t *testing.T) {
		// Call GetTreeChildDirectory() against a valid Tree
		// object. The provided BlobSlicer should be capable of
		// extracting the locations of both children.
		directory1 := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "subdirectory",
					Digest: &remoteexecution.Digest{
						Hash:      "a7536a0ebdeefa48280e135ea77755f0",
						SizeBytes: 51,
					},
				},
			},
		}
		directory2 := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "hello.txt",
					Digest: &remoteexecution.Digest{
						Hash:      "8b1a9953c4611296a827abf8c47804d7",
						SizeBytes: 5,
					},
				},
			},
		}
		tree := &remoteexecution.Tree{
			Root: &remoteexecution.Directory{
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "directory",
						Digest: &remoteexecution.Digest{
							Hash:      "ed56cd683c99acdff14b77db249819fc",
							SizeBytes: 54,
						},
					},
				},
			},
			Children: []*remoteexecution.Directory{
				directory1,
				directory2,
			},
		}
		treeDigest := digest.MustNewDigest("example", "ed56cd683c99acdff14b77db249819fc", 162)
		directory1Digest := digest.MustNewDigest("example", "5eede3f7e2a1a66c06ffd3906115a55b", 54)
		directory2Digest := digest.MustNewDigest("example", "a7536a0ebdeefa48280e135ea77755f0", 51)

		blobAccess.EXPECT().GetFromComposite(ctx, treeDigest, gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, treeDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
				// Call into the slicer to extract
				// Directory objects from the Tree.
				b, slices := slicer.Slice(buffer.NewProtoBufferFromProto(tree, buffer.UserProvided), childDigest)
				require.Equal(t, []slicing.BlobSlice{
					{
						Digest:      directory1Digest,
						OffsetBytes: 55,
						SizeBytes:   54,
					},
					{
						Digest:      directory2Digest,
						OffsetBytes: 111,
						SizeBytes:   51,
					},
				}, slices)
				return b
			}).
			AnyTimes()

		fetchedDirectory, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directory1Digest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directory1, fetchedDirectory)

		fetchedDirectory, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directory2Digest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directory2, fetchedDirectory)

		_, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			digest.MustNewDigest("example", "cb572cb90e637d1eb64c5358aa398b5e", 400))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree"), err)
	})
}
