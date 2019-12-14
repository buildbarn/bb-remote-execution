package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchedStoreBlobAccessSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess, flush := blobstore.NewBatchedStoreBlobAccess(baseBlobAccess, util.DigestKeyWithoutInstance, 2)

	// Empty calls to FindMissing() may be generated at any point in
	// time. It is up to the storage backend to filter those out.
	baseBlobAccess.EXPECT().FindMissing(ctx, []*util.Digest{}).Return(nil, nil).AnyTimes()

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "d41d8cd98f00b204e9800998ecf8427e",
			SizeBytes: 0,
		})
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestEmpty, buffer.NewValidatedBufferFromByteSlice(nil)))
	}

	digestHello := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 5,
		})
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestHello, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	}

	// Attempting to store a third blob should cause the first two
	// blobs to be flushed.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		[]*util.Digest{digestHello, digestEmpty}).Return(
		[]*util.Digest{digestHello}, nil)
	baseBlobAccess.EXPECT().Put(ctx, digestHello, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return nil
		})

	digestGoodbye := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "6fc422233a40a75a1f028e11c3cd1140",
			SizeBytes: 7,
		})
	require.NoError(t, blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))

	// Flushing should cause the third blob to be written.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		[]*util.Digest{digestGoodbye}).Return(
		[]*util.Digest{digestGoodbye}, nil)
	baseBlobAccess.EXPECT().Put(ctx, digestGoodbye, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
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
	defer ctrl.Finish()

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess, flush := blobstore.NewBatchedStoreBlobAccess(baseBlobAccess, util.DigestKeyWithoutInstance, 2)

	// Empty calls to FindMissing() may be generated at any point in
	// time. It is up to the storage backend to filter those out.
	baseBlobAccess.EXPECT().FindMissing(ctx, []*util.Digest{}).Return(nil, nil).AnyTimes()

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "d41d8cd98f00b204e9800998ecf8427e",
			SizeBytes: 0,
		})
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestEmpty, buffer.NewValidatedBufferFromByteSlice(nil)))
	}

	digestHello := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 5,
		})
	for i := 0; i < 10; i++ {
		require.NoError(t, blobAccess.Put(ctx, digestHello, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	}

	// Attempting to store a third blob should cause the first two
	// blobs to be flushed. Due to an I/O failure, we should switch
	// to an error state in which we no longer perform I/O until
	// flushed.
	baseBlobAccess.EXPECT().FindMissing(
		ctx,
		[]*util.Digest{digestHello, digestEmpty}).Return(
		[]*util.Digest{digestHello}, nil)
	baseBlobAccess.EXPECT().Put(
		ctx, digestHello, gomock.Any(),
	).DoAndReturn(func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
		data, err := b.ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return status.Error(codes.Internal, "Storage backend on fire")
	})

	digestGoodbye := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "6fc422233a40a75a1f028e11c3cd1140",
			SizeBytes: 7,
		})
	require.Equal(
		t,
		blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))),
		status.Error(codes.Internal, "Failed to store previous blob 8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"))

	// Future requests to store blobs should be discarded
	// immediately, returning same error.
	require.Equal(
		t,
		blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))),
		status.Error(codes.Internal, "Failed to store previous blob 8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"))

	// Flushing should not cause any requests on the backend, due to
	// it being in the error state. It should return the error that
	// caused it to go into the error state.
	require.Equal(t, flush(ctx), status.Error(codes.Internal, "Failed to store previous blob 8b1a9953c4611296a827abf8c47804d7-5-default: Storage backend on fire"))

	// Successive stores and flushes should be functional once again.
	require.NoError(t, blobAccess.Put(ctx, digestGoodbye, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye"))))
	baseBlobAccess.EXPECT().FindMissing(ctx, []*util.Digest{digestGoodbye}).Return(nil, nil)
	require.NoError(t, flush(ctx))
}
