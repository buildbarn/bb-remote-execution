//go:build darwin || linux
// +build darwin linux

package fuse_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCASInitialContentsFetcherFetchContents(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryWalker := mock.NewMockDirectoryWalker(ctrl)
	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	initialContentsFetcher := fuse.NewCASInitialContentsFetcher(
		ctx,
		directoryWalker,
		casFileFactory,
		digest.MustNewInstanceName("hello"))

	t.Run("DirectoryWalkerFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		directoryWalker.EXPECT().GetDirectory(ctx).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		require.Equal(t, status.Error(codes.Internal, "Root directory: Server failure"), err)
	})

	t.Run("ChildDirectoryInvalidName", func(t *testing.T) {
		// Directories containing entries with invalid names
		// should be rejected, as they cannot be instantiated.
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "..",
					Digest: &remoteexecution.Digest{
						Hash:      "4df5f448a5e6b3c41e6aae7a8a9832aa",
						SizeBytes: 123,
					},
				},
			},
		}, nil)
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		require.Equal(t, status.Error(codes.InvalidArgument, "Root directory: Directory \"..\" has an invalid name"), err)
	})

	t.Run("ChildDirectoryInvalidDigest", func(t *testing.T) {
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "Not a valid digest",
						SizeBytes: 123,
					},
				},
			},
		}, nil)
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		require.Equal(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for directory \"hello\": Unknown digest hash length: 18 characters"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Let the InitialContentsFetcher successfully parse a
		// Directory object.
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory",
					Digest: &remoteexecution.Digest{
						Hash:      "4b3b03436604cb9d831b91c71a8c1952",
						SizeBytes: 123,
					},
				},
			},
			Files: []*remoteexecution.FileNode{
				{
					Name: "executable",
					Digest: &remoteexecution.Digest{
						Hash:      "946fbe7108add776d3e3094f512c3483",
						SizeBytes: 456,
					},
					IsExecutable: true,
				},
				{
					Name: "file",
					Digest: &remoteexecution.Digest{
						Hash:      "c0607941dd5b3ca8e175a1bfbfd1c0ea",
						SizeBytes: 789,
					},
				},
			},
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target",
				},
			},
		}, nil)
		childDirectoryWalker := mock.NewMockDirectoryWalker(ctrl)
		directoryWalker.EXPECT().GetChild(digest.MustNewDigest("hello", "4b3b03436604cb9d831b91c71a8c1952", 123)).
			Return(childDirectoryWalker)
		executableLeaf := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", "946fbe7108add776d3e3094f512c3483", 456),
			true,
			gomock.Any(),
		).Return(executableLeaf)
		fileLeaf := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", "c0607941dd5b3ca8e175a1bfbfd1c0ea", 789),
			false,
			gomock.Any(),
		).Return(fileLeaf)

		children, err := initialContentsFetcher.FetchContents()
		require.NoError(t, err)
		childInitialContentsFetcher := children[path.MustNewComponent("directory")].Directory
		require.Equal(t, map[path.Component]fuse.InitialNode{
			path.MustNewComponent("directory"): {
				Directory: childInitialContentsFetcher,
			},
			path.MustNewComponent("executable"): {
				Leaf: executableLeaf,
			},
			path.MustNewComponent("file"): {
				Leaf: fileLeaf,
			},
			path.MustNewComponent("symlink"): {
				Leaf: fuse.NewSymlink("target"),
			},
		}, children)

		// Check that the InitialContentsFetcher that is created
		// for the subdirectory calls into the right DirectoryWalker.
		childDirectoryWalker.EXPECT().GetDirectory(ctx).
			Return(&remoteexecution.Directory{}, nil)

		grandchildren, err := childInitialContentsFetcher.FetchContents()
		require.NoError(t, err)
		require.Empty(t, grandchildren)
	})
}

func TestCASInitialContentsFetcherGetContainingDigests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryWalker := mock.NewMockDirectoryWalker(ctrl)
	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	initialContentsFetcher := fuse.NewCASInitialContentsFetcher(
		ctx,
		directoryWalker,
		casFileFactory,
		digest.MustNewInstanceName("hello"))

	t.Run("DirectoryWalkerFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", "7f390b0d6fb7831b0172bd7ce3e54256", 12))
		directoryWalker.EXPECT().GetDirectory(ctx).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.GetContainingDigests(ctx)
		require.Equal(t, status.Error(codes.Internal, "Root directory: Server failure"), err)
	})

	t.Run("ChildDirectoryInvalidDigest", func(t *testing.T) {
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", "7f390b0d6fb7831b0172bd7ce3e54256", 12))
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "Not a valid digest",
						SizeBytes: 123,
					},
				},
			},
		}, nil)
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.GetContainingDigests(ctx)
		require.Equal(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for directory \"hello\": Unknown digest hash length: 18 characters"), err)
	})

	t.Run("ChildFileInvalidDigest", func(t *testing.T) {
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", "7f390b0d6fb7831b0172bd7ce3e54256", 12))
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "Not a valid digest",
						SizeBytes: 123,
					},
				},
			},
		}, nil)
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.GetContainingDigests(ctx)
		require.Equal(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for file \"hello\": Unknown digest hash length: 18 characters"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully compute the transitive closure of
		// digests referenced by a directory hierarchy. Each
		// directory should only be processed once to prevent
		// exponential running times on malicious Tree objects.
		directoryDigest := digest.MustNewDigest("hello", "7f390b0d6fb7831b0172bd7ce3e54256", 12)
		directoryWalker.EXPECT().GetContainingDigest().Return(directoryDigest)
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory1",
					Digest: &remoteexecution.Digest{
						Hash:      "4b3b03436604cb9d831b91c71a8c1952",
						SizeBytes: 123,
					},
				},
				{
					Name: "directory2",
					Digest: &remoteexecution.Digest{
						Hash:      "4b3b03436604cb9d831b91c71a8c1952",
						SizeBytes: 123,
					},
				},
			},
			Files: []*remoteexecution.FileNode{
				{
					Name: "file",
					Digest: &remoteexecution.Digest{
						Hash:      "c0607941dd5b3ca8e175a1bfbfd1c0ea",
						SizeBytes: 789,
					},
				},
			},
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target",
				},
			},
		}, nil)
		childDirectoryWalker := mock.NewMockDirectoryWalker(ctrl)
		childDirectoryDigest := digest.MustNewDigest("hello", "4b3b03436604cb9d831b91c71a8c1952", 123)
		directoryWalker.EXPECT().GetChild(childDirectoryDigest).Return(childDirectoryWalker)
		childDirectoryWalker.EXPECT().GetContainingDigest().Return(childDirectoryDigest)
		childDirectoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "file",
					Digest: &remoteexecution.Digest{
						Hash:      "19dc69325bd8dfcd75cefbb6144ea3bb",
						SizeBytes: 42,
					},
				},
			},
		}, nil)

		digests, err := initialContentsFetcher.GetContainingDigests(ctx)
		require.NoError(t, err)
		require.Equal(
			t,
			digest.NewSetBuilder().
				Add(directoryDigest).
				Add(childDirectoryDigest).
				Add(digest.MustNewDigest("hello", "c0607941dd5b3ca8e175a1bfbfd1c0ea", 789)).
				Add(digest.MustNewDigest("hello", "19dc69325bd8dfcd75cefbb6144ea3bb", 42)).
				Build(),
			digests)
	})
}
