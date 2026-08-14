package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestCASDirectoryFetcherGetDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().
		FetchCDCParameters(gomock.Any(), gomock.Any()).
		Return(cdc.Parameters{MinChunkSizeBytes: 256 << 10, HorizonSizeBytes: 8 * 256 << 10}, nil).
		AnyTimes()
	directoryFetcher := cas.NewCASDirectoryFetcher(contentAddressableStorage, 1000, 10000)

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Directory object should be propagated.
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)
		contentAddressableStorage.EXPECT().
			FetchChunk(ctx, directoryDigest).
			Return(nil, status.Error(codes.Internal, "I/O error"))

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidDirectory", func(t *testing.T) {
		// It is only valid to call GetDirectory() against an
		// REv2 Directory object.
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "764b0da73352b970cfbfc488a0f54934", 30)

		contentAddressableStorage.EXPECT().
			FetchChunk(ctx, directoryDigest).
			Return([]byte("This is not a Directory object"), nil)

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: "), err)
	})

	t.Run("Success", func(t *testing.T) {
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100)
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

		dirBytes, err := proto.Marshal(exampleDirectory)
		require.NoError(t, err)

		contentAddressableStorage.EXPECT().
			FetchChunk(ctx, directoryDigest).
			Return(dirBytes, nil)

		directory, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeRootDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().
		FetchCDCParameters(gomock.Any(), gomock.Any()).
		Return(cdc.Parameters{MinChunkSizeBytes: 256 << 10, HorizonSizeBytes: 8 * 256 << 10}, nil).
		AnyTimes()
	directoryFetcher := cas.NewCASDirectoryFetcher(contentAddressableStorage, 1000, 10000)

	t.Run("TooBig", func(t *testing.T) {
		_, err := directoryFetcher.GetTreeRootDirectory(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100000))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree exceeds the maximum permitted size of 10000 bytes"), err)
	})

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Tree object should be propagated.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "I/O error"))

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidDirectory", func(t *testing.T) {
		// It is only valid to call GetTreeRootDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3478477ca0af085e8d676f9a53b095cb", 25)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return([]byte("This is not a Tree object"), nil)

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Field with number 10 at offset 0 has type 4, while 2 was expected"), err)
	})

	t.Run("MissingRootDirectory", func(t *testing.T) {
		// Malformed Tree objects may not have a root directory.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100)

		treeBytes, err := proto.Marshal(&remoteexecution.Tree{})
		require.NoError(t, err)
		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)

		_, err = directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree does not contain a root directory"), err)
	})

	t.Run("Success", func(t *testing.T) {
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100)
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

		treeBytes, err := proto.Marshal(&remoteexecution.Tree{Root: exampleDirectory})
		require.NoError(t, err)
		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)

		directory, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeChildDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().
		FetchCDCParameters(gomock.Any(), gomock.Any()).
		Return(cdc.Parameters{MinChunkSizeBytes: 256 << 10, HorizonSizeBytes: 8 * 256 << 10}, nil).
		AnyTimes()
	directoryFetcher := cas.NewCASDirectoryFetcher(contentAddressableStorage, 1000, 10000)

	t.Run("TooBig", func(t *testing.T) {
		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5959bc9570aa7909a09163bb2201f4af", 100000),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "2c09e7b2ad516c4cd9fc5c244ae08794", 100),
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree exceeds the maximum permitted size of 10000 bytes"), err)
	})

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Tree object should be propagated.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "40d8f0c70941162ee9dfacf8863d23f5", 100)
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "I/O error"))

		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directoryDigest,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidTree", func(t *testing.T) {
		// It is only valid to call GetTreeChildDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3478477ca0af085e8d676f9a53b095cb", 25)
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f297d724d679d79d577d46c79fd4d712", 10)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return([]byte("This is not a Tree object"), nil)

		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directoryDigest,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Field with number 10 at offset 0 has type 4, while 2 was expected"), err)
	})

	t.Run("ValidTree", func(t *testing.T) {
		// Call GetTreeChildDirectory() against a valid Tree
		// object. The provided BlobSlicer should be capable of
		// extracting the locations of both children.
		rootDirectory := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory",
					Digest: &remoteexecution.Digest{
						Hash:      "ed56cd683c99acdff14b77db249819fc",
						SizeBytes: 54,
					},
				},
			},
		}
		childDirectory1 := &remoteexecution.Directory{
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
		childDirectory2 := &remoteexecution.Directory{
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
			Root: rootDirectory,
			Children: []*remoteexecution.Directory{
				childDirectory1,
				childDirectory2,
			},
		}
		treeBytes, err := proto.Marshal(tree)
		require.NoError(t, err)
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "ed56cd683c99acdff14b77db249819fc", 162)
		rootDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "49aec856854ce5d7626c7153f143030c", 51)
		childDirectory1Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5eede3f7e2a1a66c06ffd3906115a55b", 54)
		childDirectory2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "a7536a0ebdeefa48280e135ea77755f0", 51)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)
		fetchedDirectory, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			rootDirectoryDigest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, rootDirectory, fetchedDirectory)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)
		fetchedDirectory, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			childDirectory1Digest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, childDirectory1, fetchedDirectory)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)
		fetchedDirectory, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			childDirectory2Digest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, childDirectory2, fetchedDirectory)

		contentAddressableStorage.EXPECT().FetchChunk(ctx, treeDigest).Return(treeBytes, nil)
		_, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "cb572cb90e637d1eb64c5358aa398b5e", 400),
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree"), err)
	})
}
