package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchedStoreBlobAccessSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	putSemaphore := semaphore.NewWeighted(1)
	blobAccess, flush := blobstore.NewBatchedStoreBlobAccess(baseBlobAccess, digest.KeyWithoutInstance, 2, putSemaphore)

	// Empty calls to FindMissing() may be generated at any point in
	// time. It is up to the storage backend to filter those out.
	baseBlobAccess.EXPECT().FindMissing(ctx, digest.EmptySet).Return(digest.EmptySet, nil).AnyTimes()

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"d41d8cd98f00b204e9800998ecf8427e",
		0)
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestEmpty, buffer.NewValidatedBufferFromByteSlice(nil)))
	}

	digestHello := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"8b1a9953c4611296a827abf8c47804d7",
		5)
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestHello, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	}

	// Attempting to store a third blob should cause the first two
	// blobs to be flushed.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		digest.NewSetBuilder().Add(digestHello).Add(digestEmpty).Build()).Return(
		digest.NewSetBuilder().Add(digestHello).Build(), nil)
	baseBlobAccess.EXPECT().Put(gomock.Any(), digestHello, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return nil
		})

	digestGoodbye := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"6fc422233a40a75a1f028e11c3cd1140",
		7)
	require.NoError(t, blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))

	// Flushing should cause the third blob to be written.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		digest.NewSetBuilder().Add(digestGoodbye).Build()).Return(
		digest.NewSetBuilder().Add(digestGoodbye).Build(), nil)
	baseBlobAccess.EXPECT().Put(gomock.Any(), digestGoodbye, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Goodbye"), data)
			return nil
		})

	require.NoError(t, flush(ctx))

	// Flushing redundantly should have no longer have any effect.
	require.NoError(t, flush(ctx))
}

func TestBatchedStoreBlobAccessFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	putSemaphore := semaphore.NewWeighted(1)
	blobAccess, flush := blobstore.NewBatchedStoreBlobAccess(baseBlobAccess, digest.KeyWithoutInstance, 2, putSemaphore)

	// Empty calls to FindMissing() may be generated at any point in
	// time. It is up to the storage backend to filter those out.
	baseBlobAccess.EXPECT().FindMissing(ctx, digest.EmptySet).Return(digest.EmptySet, nil).AnyTimes()

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"d41d8cd98f00b204e9800998ecf8427e",
		0)
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestEmpty, buffer.NewValidatedBufferFromByteSlice(nil)))
	}

	digestHello := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"8b1a9953c4611296a827abf8c47804d7",
		5)
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestHello, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	}

	// Attempting to store a third blob should cause the first two
	// blobs to be flushed. Due to an I/O failure, we should switch
	// to an error state in which we no longer perform I/O until
	// flushed.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		digest.NewSetBuilder().Add(digestHello).Add(digestEmpty).Build()).Return(
		digest.NewSetBuilder().Add(digestHello).Build(), nil)
	baseBlobAccess.EXPECT().Put(
		gomock.Any(), digestHello, gomock.Any(),
	).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		data, err := b.ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return status.Error(codes.Internal, "Storage backend on fire")
	})

	digestGoodbye := digest.MustNewDigest(
		"default",
		remoteexecution.DigestFunction_MD5,
		"6fc422233a40a75a1f028e11c3cd1140",
		7)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"),
		blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))

	// Future requests to store blobs should be discarded
	// immediately, returning same error.
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"),
		blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))

	// Flushing should not cause any requests on the backend, due to
	// it being in the error state. It should return the error that
	// caused it to go into the error state.
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"),
		flush(ctx))

	// Successive stores and flushes should be functional once again.
	require.NoError(t, blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))
	baseBlobAccess.EXPECT().FindMissing(ctx, digest.NewSetBuilder().Add(digestGoodbye).Build()).Return(digest.EmptySet, nil)
	require.NoError(t, flush(ctx))
}

func TestBatchedStoreBlobAccessCanceledWhileWaitingOnSemaphore(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	putSemaphore := semaphore.NewWeighted(0)
	blobAccess, flush := blobstore.NewBatchedStoreBlobAccess(baseBlobAccess, digest.KeyWithoutInstance, 2, putSemaphore)

	// Enqueue a blob for writing.
	digestHello := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	reader := mock.NewMockFileReader(ctrl)
	require.NoError(t, blobAccess.Put(ctx, digestHello, buffer.NewValidatedBufferFromReaderAt(reader, 5)))

	// Flushing it should attempt to write it. Because the semaphore
	// is set to zero, there is no capacity to do this. As we're
	// using a context that is canceled, this should not cause
	// flushing to block.
	ctxCanceled, cancel := context.WithCancel(ctx)
	cancel()
	baseBlobAccess.EXPECT().FindMissing(ctxCanceled, digestHello.ToSingletonSet()).Return(digestHello.ToSingletonSet(), nil)
	reader.EXPECT().Close()

	testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "context canceled"), flush(ctxCanceled))
}
