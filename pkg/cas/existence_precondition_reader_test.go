package cas_test

import (
	"context"
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

func TestExistencePreconditionReader(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("NotFound", func(t *testing.T) {
		baseReader := mock.NewMockReader[bool](ctrl)
		reader := cas.NewExistencePreconditionReader(baseReader)
		baseReader.EXPECT().Read(
			gomock.Any(),
			d,
		).Return(false, status.Error(codes.NotFound, "Blob not found"))

		_, err := reader.Read(ctx, d)
		s := status.Convert(err)
		require.Equal(t, codes.FailedPrecondition, s.Code())
		require.Equal(t, "Blob not found", s.Message())
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
	})

	t.Run("OtherCode", func(t *testing.T) {
		baseReader := mock.NewMockReader[bool](ctrl)
		reader := cas.NewExistencePreconditionReader(baseReader)
		baseReader.EXPECT().Read(
			gomock.Any(),
			d,
		).Return(false, status.Error(codes.ResourceExhausted, "Download more ram"))

		_, err := reader.Read(ctx, d)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.ResourceExhausted, "Download more ram"),
			err,
		)
	})

	t.Run("Success", func(t *testing.T) {
		baseReader := mock.NewMockReader[bool](ctrl)
		reader := cas.NewExistencePreconditionReader(baseReader)
		baseReader.EXPECT().Read(
			gomock.Any(),
			d,
		).Return(true, nil)

		value, err := reader.Read(ctx, d)
		require.True(t, value)
		require.NoError(t, err)
	})
}
