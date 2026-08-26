package cas_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchingBlobUploadSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDigestKeyFormat().Return(digest.KeyWithoutInstance)
	uploadConcurrencySemaphore := semaphore.NewWeighted(10)

	blobUploader, flush := cas.NewBatchingBlobUploader(contentAddressableStorage, 2, uploadConcurrencySemaphore)

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	digestFunction := digestEmpty.GetDigestFunction()
	for i := 0; i < 10; i++ {
		d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader(nil))
		require.NoError(t, err)
		require.Equal(t, digestEmpty, d)
	}
	digestHello := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	for i := 0; i < 10; i++ {
		d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Hello")))
		require.NoError(t, err)
		require.Equal(t, digestHello, d)
	}

	// Attempting to store a third blob should cause the first two blobs
	// to be flushed, but only digestHello is missing and needs to be
	// uploaded.
	contentAddressableStorage.EXPECT().FetchCDCParameters(gomock.Any(), gomock.Any()).Return(cdc.Parameters{
		MinChunkSizeBytes: 256 << 10,
		HorizonSizeBytes:  8 * 256 << 10,
	}, nil)
	contentAddressableStorage.EXPECT().
		FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(digestHello).Add(digestEmpty).Build()).
		Return(digestHello.ToSingletonSet(), nil)
	contentAddressableStorage.EXPECT().PutChunk(gomock.Any(), digestHello, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, data []byte) error {
			require.Equal(t, []byte("Hello"), data)
			return nil
		},
	)

	digestGoodbye := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)
	d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Goodbye")))
	require.NoError(t, err)
	require.Equal(t, digestGoodbye, d)

	// The third blob is enqueued and should be written when flushed.
	contentAddressableStorage.EXPECT().FetchCDCParameters(gomock.Any(), gomock.Any()).Return(cdc.Parameters{
		MinChunkSizeBytes: 256 << 10,
		HorizonSizeBytes:  8 * 256 << 10,
	}, nil)
	contentAddressableStorage.EXPECT().
		FindMissing(gomock.Any(), digestGoodbye.ToSingletonSet()).
		Return(digestGoodbye.ToSingletonSet(), nil)
	contentAddressableStorage.EXPECT().PutChunk(gomock.Any(), digestGoodbye, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, data []byte) error {
			require.Equal(t, []byte("Goodbye"), data)
			return nil
		},
	)

	require.NoError(t, flush(ctx))
	// Redundant flushing should cause no operations.
	require.NoError(t, flush(ctx))
}

func TestBatchingBlobUploaderFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDigestKeyFormat().Return(digest.KeyWithoutInstance)
	uploadConcurrencySemaphore := semaphore.NewWeighted(1)
	blobUploader, flush := cas.NewBatchingBlobUploader(contentAddressableStorage, 2, uploadConcurrencySemaphore)

	// We should be able to enqueue requests for up to two blobs
	// without generating any calls on the storage backend.
	digestEmpty := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	digestFunction := digestEmpty.GetDigestFunction()
	for i := 0; i < 10; i++ {
		d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader(nil))
		require.NoError(t, err)
		require.Equal(t, digestEmpty, d)
	}
	digestHello := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	for i := 0; i < 10; i++ {
		d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Hello")))
		require.NoError(t, err)
		require.Equal(t, digestHello, d)
	}

	// Attempting to store a third blob should cause the first two blobs
	// to be flushed. Due to an I/O failure, we should switch to an
	// error state in which we no longer perform I/O until flushed.
	contentAddressableStorage.EXPECT().FetchCDCParameters(gomock.Any(), gomock.Any()).Return(cdc.Parameters{
		MinChunkSizeBytes: 256 << 10,
		HorizonSizeBytes:  8 * 256 << 10,
	}, nil)
	contentAddressableStorage.EXPECT().
		FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(digestHello).Add(digestEmpty).Build()).
		Return(digest.NewSetBuilder(1).Add(digestHello).Build(), nil)
	contentAddressableStorage.EXPECT().PutChunk(gomock.Any(), digestHello, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, data []byte) error {
			require.Equal(t, []byte("Hello"), data)
			return status.Error(codes.Internal, "Storage backend on fire")
		},
	)

	digestGoodbye := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)
	_, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Goodbye")))
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Failed to save chunk: Storage backend on fire"),
		err,
	)

	// Future requests to store blobs should be discarded
	// immediately, returning same error.
	_, err = blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Goodbye")))
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Failed to save chunk: Storage backend on fire"),
		err,
	)

	// Flushing should not cause any requests on the backend, due to
	// it being in the error state. It should return the error that
	// caused it to go into the error state.
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to store previous blob 3-8b1a9953c4611296a827abf8c47804d7-5-default: Failed to save chunk: Storage backend on fire"),
		flush(ctx),
	)

	// Successive stores and flushes should be functional once again.
	d, err := blobUploader.UploadBlob(ctx, digestFunction, cas.ByteSliceFileReader([]byte("Goodbye")))
	require.NoError(t, err)
	require.Equal(t, digestGoodbye, d)
	contentAddressableStorage.EXPECT().FindMissing(ctx, digestGoodbye.ToSingletonSet()).Return(digest.EmptySet, nil)
	require.NoError(t, flush(ctx))
}

func TestBatchingBlobUploaderCanceledWhileWaitingOnSemaphore(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDigestKeyFormat().Return(digest.KeyWithoutInstance)
	uploadConcurrencySemaphore := semaphore.NewWeighted(0)
	blobUploader, flush := cas.NewBatchingBlobUploader(contentAddressableStorage, 2, uploadConcurrencySemaphore)

	// Enqueue a blob for writing.
	digestHello := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digestFunction := digestHello.GetDigestFunction()
	reader := mock.NewMockFileReader(ctrl)
	reader.EXPECT().Len().Return(int64(5), nil)
	reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
		copy(p, "Hello")
		return 5, io.EOF
	})

	d, err := blobUploader.UploadBlob(ctx, digestFunction, reader)
	require.NoError(t, err)
	require.Equal(t, digestHello, d)

	// Flushing it should attempt to write it. Because the semaphore
	// is set to zero, there is no capacity to do this. As we're
	// using a context that is canceled, this should not cause
	// flushing to block.
	ctxCanceled, cancel := context.WithCancel(ctx)
	cancel()
	contentAddressableStorage.EXPECT().FindMissing(ctxCanceled, digestHello.ToSingletonSet()).Return(digestHello.ToSingletonSet(), nil)
	reader.EXPECT().Close()

	testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "context canceled"), flush(ctxCanceled))
}

func TestBatchingBlobUploaderSuccessFileGrownDuringUpload(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDigestKeyFormat().Return(digest.KeyWithoutInstance)
	uploadConcurrencySemaphore := semaphore.NewWeighted(1)
	blobUploader, flush := cas.NewBatchingBlobUploader(contentAddressableStorage, 2, uploadConcurrencySemaphore)

	helloWorldDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)
	digestFunction := helloWorldDigest.GetDigestFunction()

	file := mock.NewMockFileReader(ctrl)
	gomock.InOrder(
		file.EXPECT().Len().Return(int64(11), nil),
		file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				require.Len(t, p, 11)
				copy(p, "Hello world")
				return 11, io.EOF
			},
		),
		file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				require.Len(t, p, 11)
				copy(p, "Hello world")
				return 11, nil
			},
		),
		file.EXPECT().Close().Return(nil),
	)

	d, err := blobUploader.UploadBlob(ctx, digestFunction, file)
	require.NoError(t, err)
	require.Equal(t, helloWorldDigest, d)

	contentAddressableStorage.EXPECT().FetchCDCParameters(gomock.Any(), gomock.Any()).Return(cdc.Parameters{
		MinChunkSizeBytes: 256 << 10,
		HorizonSizeBytes:  8 * 256 << 10,
	}, nil)
	contentAddressableStorage.EXPECT().
		FindMissing(gomock.Any(), helloWorldDigest.ToSingletonSet()).
		Return(helloWorldDigest.ToSingletonSet(), nil)
	contentAddressableStorage.EXPECT().PutChunk(gomock.Any(), helloWorldDigest, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, data []byte) error {
			require.Equal(t, []byte("Hello world"), data)
			return nil
		},
	)

	require.NoError(t, flush(ctx))
}
