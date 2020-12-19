package builder_test

import (
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCleanBuildDirectoryCreatorAcquireFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Failure to create a build directory should simply be forwarded.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false,
	).Return(nil, "", status.Error(codes.Internal, "No space left on device"))

	var initializer sync.Initializer
	buildDirectoryCreator := builder.NewCleanBuildDirectoryCreator(baseBuildDirectoryCreator, &initializer)
	_, _, err := buildDirectoryCreator.GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false)
	require.Equal(t, status.Error(codes.Internal, "No space left on device"), err)
}

func TestCleanBuildDirectoryCreatorRemoveAllChildrenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Failure to clean the build subdirectory is always an internal error.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false,
	).Return(baseBuildDirectory, "base-directory", nil)
	baseBuildDirectory.EXPECT().RemoveAllChildren().Return(
		status.Error(codes.PermissionDenied, "You don't have permissions to remove files from disk"))
	baseBuildDirectory.EXPECT().Close()

	var initializer sync.Initializer
	buildDirectoryCreator := builder.NewCleanBuildDirectoryCreator(baseBuildDirectoryCreator, &initializer)
	_, _, err := buildDirectoryCreator.GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false)
	require.Equal(t, status.Error(codes.Internal, "Failed to clean build directory \"base-directory\" prior to build: You don't have permissions to remove files from disk"), err)
}

func TestCleanBuildDirectoryCreatorSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Successful build in a clean build directory.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false,
	).Return(baseBuildDirectory, "base-directory", nil)
	baseBuildDirectory.EXPECT().RemoveAllChildren().Return(nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("hello"), os.FileMode(0700))
	baseBuildDirectory.EXPECT().Close()

	var initializer sync.Initializer
	buildDirectoryCreator := builder.NewCleanBuildDirectoryCreator(baseBuildDirectoryCreator, &initializer)
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		false)
	require.NoError(t, err)
	require.Equal(t, "base-directory", buildDirectoryPath)
	require.NoError(t, buildDirectory.Mkdir(path.MustNewComponent("hello"), os.FileMode(0700)))
	buildDirectory.Close()
}
