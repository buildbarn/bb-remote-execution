package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestExistencePreconditionContentAddressableStorageFetchChunkSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let FetchChunk succeed.
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().FetchChunk(
		ctx,
		digest.MustNewDigest("debian8", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
	).Return([]byte("Hello"), nil)

	// Result should not be modified.
	cas := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	data, err := cas.FetchChunk(ctx, digest.MustNewDigest("debian8", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5))
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)
}

func TestExistencePreconditionContentAddressableStorageFetchChunkOtherError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let FetchChunk return ResourceExhausted.
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().FetchChunk(
		ctx,
		digest.MustNewDigest("ubuntu1604", remoteexecution.DigestFunction_SHA1, "c916e71d733d06cb77a4775de5f77fd0b480a7e8", 8),
	).Return(nil, status.Error(codes.ResourceExhausted, "Out of luck!"))

	// The error should be passed through unmodified.
	cas := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	_, err := cas.FetchChunk(ctx, digest.MustNewDigest("ubuntu1604", remoteexecution.DigestFunction_SHA1, "c916e71d733d06cb77a4775de5f77fd0b480a7e8", 8))
	testutil.RequireEqualStatus(t, status.Error(codes.ResourceExhausted, "Out of luck!"), err)
}

func TestExistencePreconditionContentAddressableStorageFetchChunkNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let FetchChunk retun NotFound
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().FetchChunk(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
	).Return(nil, status.Error(codes.NotFound, "The chunk doesn't exist"))

	// The error should have been translated to FailedPrecondition.
	cas := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	_, gotErr := cas.FetchChunk(ctx, digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42))

	wantErr, err := status.New(codes.FailedPrecondition, "The chunk doesn't exist").WithDetails(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{
			{
				Type:    "MISSING",
				Subject: "blobs/b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559/42",
			},
		},
	})
	require.NoError(t, err)

	testutil.RequireEqualStatus(t, wantErr.Err(), gotErr)
}

func TestExistencePreconditionContentAddressableStorageGetManifestNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let GetManifest return NotFound
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().GetManifest(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
	).Return(nil, status.Error(codes.NotFound, "No chunk list"))

	// The error should have been translated to FailedPrecondition.
	cas := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	_, gotErr := cas.GetManifest(ctx, digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42))

	wantErr, err := status.New(codes.FailedPrecondition, "No chunk list").WithDetails(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{
			{
				Type:    "MISSING",
				Subject: "blobs/b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559/42",
			},
		},
	})
	require.NoError(t, err)

	testutil.RequireEqualStatus(t, wantErr.Err(), gotErr)
}

func TestExistencePreconditionContentAddressableStorageGetManifestSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	manifest := chunklist.ChunkList{
		{Offset: 0, Digest: digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42)},
	}

	// Let GetManifest return success.
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	blobDigest := digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "af2cc201a4f9e0e216e83bb550deeb27dd75ff25e6e4e7b0e5c9f3099f6bbf1e", 42)
	bottomCAS.EXPECT().GetManifest(ctx, blobDigest).Return(manifest, nil)

	// Result should not have been modified.
	cas := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	got, err := cas.GetManifest(ctx, blobDigest)
	require.NoError(t, err)
	require.Equal(t, manifest, got)
}

func TestExistencePreconditionContentAddressableStoragePutChunkNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let PutChunk return NotFound.
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().PutChunk(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
		[]byte("chunk data"),
	).Return(status.Error(codes.NotFound, "Underlying storage not found"))

	// For write operations, the error should NOT be translated to
	// FailedPrecondition.
	casStore := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	gotErr := casStore.PutChunk(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
		[]byte("chunk data"),
	)

	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Underlying storage not found"), gotErr)
}

func TestExistencePreconditionContentAddressableStoragePutManifestNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	manifest := chunklist.ChunkList{
		{Offset: 0, Digest: digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42)},
	}

	// Let PutManifest return NotFound.
	bottomCAS := mock.NewMockContentAddressableStorage(ctrl)
	bottomCAS.EXPECT().PutManifest(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
		manifest,
	).Return(status.Error(codes.NotFound, "Underlying storage not found"))

	// For write operations, the error should NOT be translated to
	// FailedPrecondition.
	casStore := cas.NewExistencePreconditionContentAddressableStorage(bottomCAS)
	gotErr := casStore.PutManifest(
		ctx,
		digest.MustNewDigest("gentoo", remoteexecution.DigestFunction_SHA256, "b5c12f3689d12ddc51a4a21cc7d649037c125645ed81f3ec32cb69b3997b7559", 42),
		manifest,
	)

	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Underlying storage not found"), gotErr)
}
