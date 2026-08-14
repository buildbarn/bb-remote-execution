package cas_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFailedPreconditionOnMissingBlob(t *testing.T) {
	t.Run("NotFound", func(t *testing.T) {
		d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		err := cas.FailedPreconditionOnMissingBlob(
			d,
			status.Error(codes.NotFound, "Blob not found"),
		)
		// The PreconditionFailure detail should name the blob.
		s := status.Convert(err)
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
		d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		err := cas.FailedPreconditionOnMissingBlob(
			d,
			status.Error(codes.ResourceExhausted, "Out of luck!"),
		)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.ResourceExhausted, "Out of luck!"),
			err,
		)
	})
}
