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
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPutBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	casBackend := mock.NewMockContentAddressableStorage(ctrl)

	d := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("FetchCDCParametersFailure", func(t *testing.T) {
		// Verify that a failure to fetch CDC parameters correctly halts
		// execution and propagates the error, inherently relying on
		// PutBlob to discard the blob.
		blob := cas.NewBlobFromByteslice([]byte("Hello"))

		casBackend.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).
			Return(cdc.Parameters{}, status.Error(codes.Internal, "Backend offline"))

		err := cas.PutBlob(ctx, casBackend, d, blob)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Could not fetch CDC parameters: Backend offline"), err)
	})

	t.Run("SingleChunkUpload", func(t *testing.T) {
		// Verify that a blob smaller than the single chunk threshold is
		// successfully extracted via ToByteSlice and inserted directly
		// into PutChunk.
		blob := cas.NewBlobFromByteslice([]byte("Hello"))

		casBackend.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).
			Return(cdc.Parameters{
				MinChunkSizeBytes: 256 << 10, // 256 KB
			}, nil)

		casBackend.EXPECT().PutChunk(ctx, d, []byte("Hello")).Return(nil)

		err := cas.PutBlob(ctx, casBackend, d, blob)
		require.NoError(t, err)
	})

	t.Run("MultiChunkStreamSuccess", func(t *testing.T) {
		// Verify that a blob larger than the single chunk threshold
		// triggers the upload of all its chunk and its chunk list.
		largeDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "fbaf48ec981a5eecdb57b929fdd426e8", 200)
		blob := cas.NewBlobFromByteslice(make([]byte, 200))

		casBackend.EXPECT().FetchCDCParameters(ctx, largeDigest.GetInstanceName()).
			Return(cdc.Parameters{
				MinChunkSizeBytes: 64,
				HorizonSizeBytes:  128,
			}, nil).Times(2)

		casBackend.EXPECT().PutChunk(ctx, gomock.Any(), gomock.Any()).
			Return(nil).Times(3)

		casBackend.EXPECT().PutManifest(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil)

		err := cas.PutBlob(ctx, casBackend, largeDigest, blob)
		require.NoError(t, err)
	})

	t.Run("MultiChunkStreamFailure", func(t *testing.T) {
		// Verify that a blob larger than the single chunk threshold
		// triggers the ToReaderAt streaming path (which eventually
		// delegates to cdc.PutReader). We simulate a chunk upload
		// failure to ensure the stream correctly surfaces it.
		largeDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_MD5, "fbaf48ec981a5eecdb57b929fdd426e8", 200)
		blob := cas.NewBlobFromByteslice(make([]byte, 200))

		casBackend.EXPECT().FetchCDCParameters(ctx, largeDigest.GetInstanceName()).
			Return(cdc.Parameters{
				MinChunkSizeBytes: 64,
				HorizonSizeBytes:  128,
			}, nil).Times(2)

		// We mock the first PutChunk to fail, verifying the error
		// bubbles up safely.
		casBackend.EXPECT().PutChunk(ctx, gomock.Any(), gomock.Any()).
			Return(status.Error(codes.Internal, "Server on fire"))

		err := cas.PutBlob(ctx, casBackend, largeDigest, blob)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to save chunk: Server on fire"), err)
	})
}
