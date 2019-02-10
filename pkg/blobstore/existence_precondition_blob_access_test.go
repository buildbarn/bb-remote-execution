package blobstore_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExistencePreconditionBlobAccessGetSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Let Get() return a reader from which we can read successfully.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx, util.MustNewDigest("debian8", &remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 5,
		})).Return(int64(5), ioutil.NopCloser(bytes.NewBufferString("Hello")), nil)

	// Validate that the reader can still be read properly.
	length, r, err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx, util.MustNewDigest("debian8", &remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 5,
		}))
	require.NoError(t, err)
	require.Equal(t, int64(5), length)
	buf, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), buf)
	require.NoError(t, r.Close())
}

func TestExistencePreconditionBlobAccessGetResourceExhausted(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Let Get() return ResourceExhausted.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "c916e71d733d06cb77a4775de5f77fd0b480a7e8",
			SizeBytes: 8,
		})).Return(int64(0), nil, status.Error(codes.ResourceExhausted, "Out of luck!"))

	// The error should be passed through unmodified.
	_, _, err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "c916e71d733d06cb77a4775de5f77fd0b480a7e8",
			SizeBytes: 8,
		}))
	s := status.Convert(err)
	require.Equal(t, codes.ResourceExhausted, s.Code())
	require.Equal(t, "Out of luck!", s.Message())
}

func TestExistencePreconditionBlobAccessGetNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Let Get() return NotFound.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Get(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4",
			SizeBytes: 7,
		})).Return(int64(0), nil, status.Error(codes.NotFound, "Blob doesn't exist!"))

	// The error should be translated to FailedPrecondition.
	_, _, err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Get(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4",
			SizeBytes: 7,
		}))
	s := status.Convert(err)
	require.Equal(t, codes.FailedPrecondition, s.Code())
	require.Equal(t, "Blob doesn't exist!", s.Message())

	// Metadata of the error should indicate which blob is missing.
	details := s.Details()
	require.Equal(t, 1, len(details))
	require.Equal(t, details[0], &errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{
			{
				Type:    "MISSING",
				Subject: "blobs/c015ad6ddaf8bb50689d2d7cbf1539dff6dd84473582a08ed1d15d841f4254f4/7",
			},
		},
	})
}

func TestExistencePreconditionBlobAccessPutNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Let Put() return NotFound.
	bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
	bottomBlobAccess.EXPECT().Put(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "89d5739baabbbe65be35cbe61c88e06d",
			SizeBytes: 6,
		}), int64(6), gomock.Any()).Return(status.Error(codes.NotFound, "Storage backend not found"))

	// Unlike for Get(), the error should be passed through
	// unmodified. This adapter should only alter the results of
	// Get() calls.
	err := blobstore.NewExistencePreconditionBlobAccess(bottomBlobAccess).Put(
		ctx, util.MustNewDigest("ubuntu1604", &remoteexecution.Digest{
			Hash:      "89d5739baabbbe65be35cbe61c88e06d",
			SizeBytes: 6,
		}), 6, ioutil.NopCloser(bytes.NewBufferString("Foobar")))
	s := status.Convert(err)
	require.Equal(t, codes.NotFound, s.Code())
	require.Equal(t, "Storage backend not found", s.Message())
}
