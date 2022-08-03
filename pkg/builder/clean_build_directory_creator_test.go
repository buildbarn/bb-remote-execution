package builder_test

import (
	"context"
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCleanBuildDirectoryCreator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseCleaner := mock.NewMockCleaner(ctrl)
	buildDirectoryCreator := builder.NewCleanBuildDirectoryCreator(baseBuildDirectoryCreator, cleaner.NewIdleInvoker(baseCleaner.Call))

	t.Run("CleanerAcquireFailure", func(t *testing.T) {
		// Failure to clean prior to acquiring a build directory
		// should be propagated.
		baseCleaner.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Cannot remove files from build directory"))

		_, _, err := buildDirectoryCreator.GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to clean before acquiring build directory: Cannot remove files from build directory"), err)
	})

	t.Run("GetBuildDirectoryFailure", func(t *testing.T) {
		// If we fail to get the underlying build directory, we
		// should release the cleaner, as its use count becomes
		// invalid afterwards. This should trigger another
		// clean.
		baseCleaner.EXPECT().Call(ctx)
		baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false,
		).Return(nil, nil, status.Error(codes.Internal, "No space left on device"))
		baseCleaner.EXPECT().Call(ctx)

		_, _, err := buildDirectoryCreator.GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false)
		require.Equal(t, status.Error(codes.Internal, "No space left on device"), err)
	})

	t.Run("CloseFailure", func(t *testing.T) {
		// Successfully obtain a build directory.
		baseCleaner.EXPECT().Call(ctx)
		baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
		baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false,
		).Return(baseBuildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), nil)

		buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false)
		require.NoError(t, err)
		require.Equal(t, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), buildDirectoryPath)

		// Validate that calls against the directory are forwarded.
		baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("hello"), os.FileMode(0o700))

		require.NoError(t, buildDirectory.Mkdir(path.MustNewComponent("hello"), os.FileMode(0o700)))

		// In case closing the directory fails, we should still
		// release the IdleInvoker, causing another clean to be
		// performed. Without it, the reference count on the
		// IdleInvoker would leak.
		baseBuildDirectory.EXPECT().Close().Return(status.Error(codes.Internal, "Failed to flush data"))
		baseCleaner.EXPECT().Call(ctx)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to flush data"),
			buildDirectory.Close())
	})

	t.Run("CleanerReleaseFailure", func(t *testing.T) {
		// Successfully obtain a build directory.
		baseCleaner.EXPECT().Call(ctx)
		baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
		baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false,
		).Return(baseBuildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), nil)

		buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false)
		require.NoError(t, err)
		require.Equal(t, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), buildDirectoryPath)

		// Cleanup failures at the end of a build should also be
		// propagated properly.
		baseBuildDirectory.EXPECT().Close()
		baseCleaner.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Failed to remove files"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to clean after releasing build directory: Failed to remove files"),
			buildDirectory.Close())
	})

	t.Run("CloseSuccess", func(t *testing.T) {
		// Successfully obtain a build directory.
		baseCleaner.EXPECT().Call(ctx)
		baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
		baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false,
		).Return(baseBuildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), nil)

		buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
			ctx,
			digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
			false)
		require.NoError(t, err)
		require.Equal(t, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), buildDirectoryPath)

		// Let both releasing of the build directory and running
		// the cleaner succeed.
		baseBuildDirectory.EXPECT().Close()
		baseCleaner.EXPECT().Call(ctx)

		require.NoError(t, buildDirectory.Close())
	})
}
