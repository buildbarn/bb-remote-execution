package cas_test

import (
	"context"
	"os"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHardlinkingFileFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseFileFetcher := mock.NewMockFileFetcher(ctrl)
	cacheDirectory := mock.NewMockDirectory(ctrl)
	fileFetcher := cas.NewHardlinkingFileFetcher(baseFileFetcher, cacheDirectory, 1, 1024, eviction.NewLRUSet())

	blobDigest1 := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)
	buildDirectory := mock.NewMockDirectory(ctrl)

	// Errors fetching files from the backend should be propagated.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false).
		Return(status.Error(codes.Internal, "Server not reachable"))
	require.Equal(
		t,
		status.Error(codes.Internal, "Server not reachable"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Failing to link the file into the cache should not cause the
	// file to become cached immediately.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	require.Equal(
		t,
		status.Error(codes.Internal, "Failed to add cached file \"8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Successfully link the file into the cache.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Once the file is cached, hardlinks should be made in the
	// opposite direction.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Failure when accessing a cached file.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.EIO)
	require.Equal(
		t,
		status.Error(codes.Internal, "Failed to create hardlink to cached file \"8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Recover from the case where the cache directory gets cleaned
	// up by another process. If hardlinking returns ENOENT, we
	// should fall back to downloading and reinserting the file.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// The above may happen in multiple threads at the same time.
	// EEXIST errors should be ignored in that case.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(os.ErrExist)
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	// Errors other than EEXIST should be propagated as usual.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	require.Equal(
		t,
		status.Error(codes.Internal, "Failed to repair cached file \"8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false))

	blobDigest2 := digest.MustNewDigest("example", "6fc422233a40a75a1f028e11c3cd1140", 7)

	// Errors other than ENOENT when removing a file should be
	// propagated, as we don't want to silently fill up the disk.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false)
	cacheDirectory.EXPECT().Remove(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	require.Equal(
		t,
		status.Error(codes.Internal, "Failed to remove cached file \"8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false))

	// ENOENT errors when removing files should be tolerated. It
	// simply means that files in the cache directory were cleaned
	// up by another process.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false)
	cacheDirectory.EXPECT().Remove(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.ENOENT)
	buildDirectory.EXPECT().Link(path.MustNewComponent("goodbye.txt"), cacheDirectory, path.MustNewComponent("6fc422233a40a75a1f028e11c3cd1140-7-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false))
}
