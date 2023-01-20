package virtual_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCASInitialContentsFetcherFetchContents(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryWalker := mock.NewMockDirectoryWalker(ctrl)
	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	initialContentsFetcher := virtual.NewCASInitialContentsFetcher(
		ctx,
		directoryWalker,
		casFileFactory,
		symlinkFactory,
		digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5))

	t.Run("DirectoryWalkerFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		directoryWalker.EXPECT().GetDirectory(ctx).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Root directory: Server failure"), err)
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Directory \"..\" has an invalid name"), err)
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for directory \"hello\": Hash has length 18, while 32 characters were expected"), err)
	})

	t.Run("ChildFileInvalidDigest", func(t *testing.T) {
		// If an error occurs after creating the first file, any
		// previously created files should be unlinked prior to
		// returning, so that the files don't leak.
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "file1",
					Digest: &remoteexecution.Digest{
						Hash:      "ded43ceff96666255cbb89a40cb9d1bd",
						SizeBytes: 1200,
					},
				},
				{
					Name: "file2",
					Digest: &remoteexecution.Digest{
						Hash:      "Not a valid digest",
						SizeBytes: 1300,
					},
				},
			},
		}, nil)
		file1 := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "ded43ceff96666255cbb89a40cb9d1bd", 1200),
			false,
		).Return(file1)
		file1.EXPECT().Unlink()
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for file \"file2\": Hash has length 18, while 32 characters were expected"), err)
	})

	t.Run("DuplicateNames", func(t *testing.T) {
		directoryWalker.EXPECT().GetDirectory(ctx).Return(&remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "0970ca3d192dde1268a19b44bbecadcf",
						SizeBytes: 3000,
					},
				},
			},
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "hello",
					Target: "target",
				},
			},
		}, nil)
		file1 := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "0970ca3d192dde1268a19b44bbecadcf", 3000),
			false,
		).Return(file1)
		file1.EXPECT().Unlink()
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.FetchContents()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Directory contains multiple children named \"hello\""), err)
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
		directoryWalker.EXPECT().GetChild(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4b3b03436604cb9d831b91c71a8c1952", 123)).
			Return(childDirectoryWalker)
		executableLeaf := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "946fbe7108add776d3e3094f512c3483", 456),
			true,
		).Return(executableLeaf)
		fileLeaf := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "c0607941dd5b3ca8e175a1bfbfd1c0ea", 789),
			false,
		).Return(fileLeaf)
		symlinkLeaf := mock.NewMockNativeLeaf(ctrl)
		symlinkFactory.EXPECT().LookupSymlink([]byte("target")).Return(symlinkLeaf)

		children, err := initialContentsFetcher.FetchContents()
		require.NoError(t, err)
		childInitialContentsFetcher, _ := children[path.MustNewComponent("directory")].GetPair()
		require.Equal(t, map[path.Component]virtual.InitialNode{
			path.MustNewComponent("directory"):  virtual.InitialNode{}.FromDirectory(childInitialContentsFetcher),
			path.MustNewComponent("executable"): virtual.InitialNode{}.FromLeaf(executableLeaf),
			path.MustNewComponent("file"):       virtual.InitialNode{}.FromLeaf(fileLeaf),
			path.MustNewComponent("symlink"):    virtual.InitialNode{}.FromLeaf(symlinkLeaf),
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
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	initialContentsFetcher := virtual.NewCASInitialContentsFetcher(
		ctx,
		directoryWalker,
		casFileFactory,
		symlinkFactory,
		digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5))

	t.Run("DirectoryWalkerFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7f390b0d6fb7831b0172bd7ce3e54256", 12))
		directoryWalker.EXPECT().GetDirectory(ctx).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		directoryWalker.EXPECT().GetDescription().Return("Root directory")

		_, err := initialContentsFetcher.GetContainingDigests(ctx)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Root directory: Server failure"), err)
	})

	t.Run("ChildDirectoryInvalidDigest", func(t *testing.T) {
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7f390b0d6fb7831b0172bd7ce3e54256", 12))
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for directory \"hello\": Hash has length 18, while 32 characters were expected"), err)
	})

	t.Run("ChildFileInvalidDigest", func(t *testing.T) {
		directoryWalker.EXPECT().GetContainingDigest().
			Return(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7f390b0d6fb7831b0172bd7ce3e54256", 12))
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Root directory: Failed to obtain digest for file \"hello\": Hash has length 18, while 32 characters were expected"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully compute the transitive closure of
		// digests referenced by a directory hierarchy. Each
		// directory should only be processed once to prevent
		// exponential running times on malicious Tree objects.
		directoryDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7f390b0d6fb7831b0172bd7ce3e54256", 12)
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
		childDirectoryDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4b3b03436604cb9d831b91c71a8c1952", 123)
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
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "c0607941dd5b3ca8e175a1bfbfd1c0ea", 789)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "19dc69325bd8dfcd75cefbb6144ea3bb", 42)).
				Build(),
			digests)
	})
}
