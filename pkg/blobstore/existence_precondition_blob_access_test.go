package blobstore_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExistencePreconditionBlobAccessGetSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let Get() return a reader from which we can read successfully.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx,
		digest.MustNewDigest("debian8", "8b1a9953c4611296a827abf8c47804d7", 5),
	).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	// Validate that the reader can still be read properly.
	data, err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx,
		digest.MustNewDigest("debian8", "8b1a9953c4611296a827abf8c47804d7", 5),
	).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)
}

func TestExistencePreconditionBlobAccessGetResourceExhausted(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let Get() return ResourceExhausted.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx,
		digest.MustNewDigest("ubuntu1604", "c916e71d733d06cb77a4775de5f77fd0b480a7e8", 8),
	).Return(buffer.NewBufferFromError(status.Error(codes.ResourceExhausted, "Out of luck!")))

	// The error should be passed through unmodified.
	_, err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx,
		digest.MustNewDigest("ubuntu1604", "c916e71d733d06cb77a4775de5f77fd0b480a7e8", 8),
	).ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.ResourceExhausted, "Out of luck!"), err)
}

func TestExistencePreconditionBlobAccessGetNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let Get() return NotFound.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx,
		digest.MustNewDigest("ubuntu1604", "c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4", 7),
	).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob doesn't exist!")))

	// The error should be translated to FailedPrecondition.
	_, gotErr := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx,
		digest.MustNewDigest("ubuntu1604", "c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4", 7),
	).ToByteSlice(100)

	wantErr, err := status.New(codes.FailedPrecondition, "Blob doesn't exist!").WithDetails(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{
			{
				Type:    "MISSING",
				Subject: "blobs/c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4/7",
			},
		},
	})
	require.NoError(t, err)

	testutil.RequireEqualStatus(t, wantErr.Err(), gotErr)
}

func TestExistencePreconditionBlobAccessPutNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Let Put() return NotFound.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Put(
		ctx,
		digest.MustNewDigest("ubuntu1604", "89d5739baabbbe65be35cbe61c88e06d", 6),
		gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Foobar"), data)
			return status.Error(codes.NotFound, "Storage backend not found")
		})

	// Unlike for Get(), the error should be passed through
	// unmodified. This adapter should only alter the results of
	// Get() calls.
	err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Put(
		ctx,
		digest.MustNewDigest("ubuntu1604", "89d5739baabbbe65be35cbe61c88e06d", 6),
		buffer.NewValidatedBufferFromByteSlice([]byte("Foobar")))
	s := status.Convert(err)
	require.Equal(t, codes.NotFound, s.Code())
	require.Equal(t, "Storage backend not found", s.Message())
}
