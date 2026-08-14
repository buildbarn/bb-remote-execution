package cas_test

import (
	"context"
	"io"
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExistencePreconditionStreamReader(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("ImmediateNotFound", func(t *testing.T) {
		baseStreamReader := mock.NewMockStreamReader(ctrl)
		streamReader := cas.NewExistencePreconditionStreamReader(baseStreamReader)
		baseStreamReader.EXPECT().ReadStream(ctx, d).Return(nil, status.Error(codes.NotFound, "Blob not found"))

		_, err := streamReader.ReadStream(ctx, d)
		requireBlobMissingPreconditionFailure(t, err, d)
	})

	t.Run("ImmediateOtherCode", func(t *testing.T) {
		baseStreamReader := mock.NewMockStreamReader(ctrl)
		streamReader := cas.NewExistencePreconditionStreamReader(baseStreamReader)
		baseStreamReader.EXPECT().ReadStream(ctx, d).Return(nil, status.Error(codes.ResourceExhausted, "Download more RAM"))

		_, err := streamReader.ReadStream(ctx, d)
		testutil.RequireEqualStatus(t, status.Error(codes.ResourceExhausted, "Download more RAM"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseStreamReader := mock.NewMockStreamReader(ctrl)
		streamReader := cas.NewExistencePreconditionStreamReader(baseStreamReader)
		baseStreamReader.EXPECT().ReadStream(ctx, d).Return(io.NopCloser(strings.NewReader("Hello")), nil)

		s, err := streamReader.ReadStream(ctx, d)
		require.NoError(t, err)
		contents, err := io.ReadAll(s)
		require.NoError(t, err)
		require.Equal(t, "Hello", string(contents))
	})

	t.Run("DeferredNotFound", func(t *testing.T) {
		baseStreamReader := mock.NewMockStreamReader(ctrl)
		streamReader := cas.NewExistencePreconditionStreamReader(baseStreamReader)
		baseStreamReader.EXPECT().ReadStream(ctx, d).Return(
			io.MultiReader(strings.NewReader("Hello "), &errorReader{err: status.Error(codes.NotFound, "Chunk not found")}),
			nil,
		)

		s, err := streamReader.ReadStream(ctx, d)
		require.NoError(t, err)
		contents, err := io.ReadAll(s)
		require.Equal(t, "Hello ", string(contents))
		requireBlobMissingPreconditionFailure(t, err, d)
	})

	t.Run("DeferredOtherCode", func(t *testing.T) {
		baseStreamReader := mock.NewMockStreamReader(ctrl)
		streamReader := cas.NewExistencePreconditionStreamReader(baseStreamReader)
		baseStreamReader.EXPECT().ReadStream(ctx, d).Return(
			io.MultiReader(strings.NewReader("Hello "), &errorReader{err: status.Error(codes.Internal, "Storage on fire")}),
			nil,
		)

		s, err := streamReader.ReadStream(ctx, d)
		require.NoError(t, err)
		contents, err := io.ReadAll(s)
		require.Equal(t, "Hello ", string(contents))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Storage on fire"), err)
	})
}

// errorReader is an io.Reader that returns an error immediately.
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// requireBlobMissingPreconditionFailure checks that err is a
// FAILED_PRECONDITION error with a PreconditionFailure detail that
// identifies the given digest as being missing.
func requireBlobMissingPreconditionFailure(t *testing.T, err error, d digest.Digest) {
	t.Helper()
	s := status.Convert(err)
	require.Equal(t, codes.FailedPrecondition, s.Code())
	require.Len(t, s.Details(), 1)
	preconditionFailure, ok := s.Details()[0].(*errdetails.PreconditionFailure)
	require.True(t, ok)
	require.Len(t, preconditionFailure.Violations, 1)
	require.Equal(t, "MISSING", preconditionFailure.Violations[0].Type)
	require.Equal(
		t,
		digest.NewInstanceNamePatcher(d.GetInstanceName(), digest.EmptyInstanceName).
			PatchDigest(d).
			GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
		preconditionFailure.Violations[0].Subject,
	)
}
