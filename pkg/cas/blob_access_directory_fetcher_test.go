package cas_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestBlobAccessDirectoryFetcherGetDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryReader := mock.NewMockMessageReader[*remoteexecution.Directory](ctrl)
	treeReader := mock.NewMockStreamReader(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, directoryReader, treeReader, 1000, 10000)

	t.Run("Error", func(t *testing.T) {
		// Failures reading the Directory object should be propagated.
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)

		directoryReader.EXPECT().ReadMessage(ctx, directoryDigest).Return(nil, status.Error(codes.Internal, "Some error reading directory object."))

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Some error reading directory object."), err)
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

		directoryReader.EXPECT().ReadMessage(ctx, directoryDigest).Return(exampleDirectory, nil)

		directory, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeRootDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryReader := mock.NewMockMessageReader[*remoteexecution.Directory](ctrl)
	treeReader := mock.NewMockStreamReader(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, directoryReader, treeReader, 1000, 10000)

	t.Run("TooBig", func(t *testing.T) {
		_, err := directoryFetcher.GetTreeRootDirectory(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100000))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree exceeds the maximum permitted size of 10000 bytes"), err)
	})

	t.Run("IOError", func(t *testing.T) {
		// Failures reading the Tree object should be propagated.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)

		errReader := mock.NewMockReadCloser(ctrl)
		errReader.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "I/O error")).AnyTimes()
		errReader.EXPECT().Close()

		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(errReader, nil)

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidDirectory", func(t *testing.T) {
		// It is only valid to call GetTreeRootDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3478477ca0af085e8d676f9a53b095cb", 25)

		r := io.NopCloser(bytes.NewReader([]byte("This is not a Tree object")))
		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(r, nil)

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Field with number 10 at offset 0 has type 4, while 2 was expected"), err)
	})

	t.Run("MissingRootDirectory", func(t *testing.T) {
		// Malformed Tree objects may not have a root directory.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f5f634611dd11ccba54c7b9d9607c3c2", 100)

		treeBytes, err := proto.Marshal(&remoteexecution.Tree{})
		require.NoError(t, err)
		r := io.NopCloser(bytes.NewReader(treeBytes))
		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(r, nil)

		_, err = directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tree does not contain a root directory"), err)
	})

	t.Run("ChecksumMismatch", func(t *testing.T) {
		// If an REv2 Tree object cannot be parsed, it must be
		// read in its entirety to ensure this isn't caused by a
		// checksum mismatch.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "ceb78ab91c6d580aceea6618dd6fc5cc", 10000)

		mockStream := mock.NewMockReadCloser(ctrl)
		gomock.InOrder(
			mockStream.EXPECT().Read(gomock.Any()).
				DoAndReturn(func(p []byte) (int, error) {
					return copy(p, "garbage data that does not parse as a proto object"), nil
				}),
			mockStream.EXPECT().Read(gomock.Any()).
				DoAndReturn(func(p []byte) (int, error) {
					return 0, status.Error(codes.InvalidArgument, "Data was garbage earlier, but the stream eventually discovered there was a checksum mismatch")
				}),
			mockStream.EXPECT().Close(),
		)
		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(mockStream, nil)

		_, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Data was garbage earlier, but the stream eventually discovered there was a checksum mismatch"), err)
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
		treeBytes, err := proto.Marshal(&remoteexecution.Tree{
			Root: exampleDirectory,
		})
		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(io.NopCloser(bytes.NewReader(treeBytes)), nil)

		directory, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, directory)
	})
}

func TestBlobAccessDirectoryFetcherGetTreeChildDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	directoryReader := mock.NewMockMessageReader[*remoteexecution.Directory](ctrl)
	treeReader := mock.NewMockStreamReader(ctrl)
	directoryFetcher := cas.NewBlobAccessDirectoryFetcher(blobAccess, directoryReader, treeReader, 1000, 10000)

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

		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "I/O error")).AnyTimes()
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
			directoryDigest,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("InvalidTree", func(t *testing.T) {
		// It is only valid to call GetTreeChildDirectory()
		// against an REv2 Tree object.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3478477ca0af085e8d676f9a53b095cb", 25)
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "f297d724d679d79d577d46c79fd4d712", 10)

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
			directoryDigest,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Field with number 10 at offset 0 has type 4, while 2 was expected"), err)
	})

	t.Run("ChecksumMismatch", func(t *testing.T) {
		// If an REv2 Tree object cannot be parsed, it must be
		// read in its entirety to ensure this isn't caused by a
		// checksum mismatch.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "ceb78ab91c6d580aceea6618dd6fc5cc", 10000)
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "138f65a6fb46dc6d97618a24b4490c19", 10)

		blobAccess.EXPECT().GetFromComposite(ctx, treeDigest, directoryDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, treeDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
				b, slices := slicer.Slice(buffer.NewCASBufferFromReader(treeDigest, io.NopCloser(bytes.NewBuffer(make([]byte, 10000))), buffer.UserProvided), childDigest)
				require.Empty(t, slices)
				return b
			})

		_, err := directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			directoryDigest,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Buffer has checksum b85d6fb9ef4260dcf1ce0a1b0bff80d3, while ceb78ab91c6d580aceea6618dd6fc5cc was expected"), err)
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
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "ed56cd683c99acdff14b77db249819fc", 162)
		rootDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "49aec856854ce5d7626c7153f143030c", 51)
		childDirectory1Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5eede3f7e2a1a66c06ffd3906115a55b", 54)
		childDirectory2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "a7536a0ebdeefa48280e135ea77755f0", 51)

		blobAccess.EXPECT().GetFromComposite(ctx, treeDigest, gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, treeDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
				// Call into the slicer to extract
				// Directory objects from the Tree.
				b, slices := slicer.Slice(buffer.NewProtoBufferFromProto(tree, buffer.UserProvided), childDigest)
				require.Equal(t, []slicing.BlobSlice{
					{
						Digest:      rootDirectoryDigest,
						OffsetBytes: 2,
						SizeBytes:   51,
					},
					{
						Digest:      childDirectory1Digest,
						OffsetBytes: 55,
						SizeBytes:   54,
					},
					{
						Digest:      childDirectory2Digest,
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
			rootDirectoryDigest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, rootDirectory, fetchedDirectory)

		fetchedDirectory, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			childDirectory1Digest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, childDirectory1, fetchedDirectory)

		fetchedDirectory, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			childDirectory2Digest,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, childDirectory2, fetchedDirectory)

		_, err = directoryFetcher.GetTreeChildDirectory(
			ctx,
			treeDigest,
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "cb572cb90e637d1eb64c5358aa398b5e", 400),
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree"), err)
	})
}
