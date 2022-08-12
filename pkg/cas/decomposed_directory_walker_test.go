package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDecomposedDirectoryWalker(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	parentDigest := digest.MustNewDigest("example", "6884a9e20905b512d1122a2b1ad8ba16", 123)
	parentDirectoryWalker := cas.NewDecomposedDirectoryWalker(directoryFetcher, parentDigest)

	exampleDirectory := &remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "foo",
				Digest: &remoteexecution.Digest{
					Hash:      "4df5f448a5e6b3c41e6aae7a8a9832aa",
					SizeBytes: 456,
				},
			},
		},
	}

	// Test that the DirectoryWalker loads the right object from the
	// CAS, and that error messages use the right prefix.

	t.Run("ParentGetDirectorySuccess", func(t *testing.T) {
		directoryFetcher.EXPECT().Get(ctx, parentDigest).
			Return(exampleDirectory, nil)
		parentDirectory, err := parentDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, parentDirectory)
	})

	t.Run("ParentGetDirectoryFailure", func(t *testing.T) {
		directoryFetcher.EXPECT().Get(ctx, parentDigest).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		_, err := parentDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.Internal, "Server failure"), err)
	})

	t.Run("ParentGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Directory \"6884a9e20905b512d1122a2b1ad8ba16-123-example\"",
			parentDirectoryWalker.GetDescription())
	})

	t.Run("ParentGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			parentDigest,
			parentDirectoryWalker.GetContainingDigest())
	})

	childDigest := digest.MustNewDigest("example", "4df5f448a5e6b3c41e6aae7a8a9832aa", 456)
	childDirectoryWalker := parentDirectoryWalker.GetChild(childDigest)

	// Repeat the tests above against a child directory, to make
	// sure those also load the right object from the CAS.

	t.Run("ChildGetDirectory", func(t *testing.T) {
		directoryFetcher.EXPECT().Get(ctx, childDigest).
			Return(exampleDirectory, nil)
		childDirectory, err := childDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleDirectory, childDirectory)
	})

	t.Run("ChildGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Directory \"4df5f448a5e6b3c41e6aae7a8a9832aa-456-example\"",
			childDirectoryWalker.GetDescription())
	})

	t.Run("ChildGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			childDigest,
			childDirectoryWalker.GetContainingDigest())
	})
}
