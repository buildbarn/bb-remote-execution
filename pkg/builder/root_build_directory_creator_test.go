package builder_test

import (
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRootBuildDirectoryCreator(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator := builder.NewRootBuildDirectoryCreator(mockBuildDirectory)

	// Run a simple build action that only performs an Mkdir() call.
	// Once terminated, the underlying build directory should not be
	// closed, as it is reused by the next build action.
	mockBuildDirectory.EXPECT().Mkdir("hello", os.FileMode(0700))
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		true)
	require.NoError(t, err)
	require.Equal(t, ".", buildDirectoryPath)
	require.NoError(t, buildDirectory.Mkdir("hello", os.FileMode(0700)))
	buildDirectory.Close()

	// Run an action similar to the previous one. It should be
	// applied against the same underlying build directory.
	mockBuildDirectory.EXPECT().Mkdir("world", os.FileMode(0700))
	buildDirectory, buildDirectoryPath, err = buildDirectoryCreator.GetBuildDirectory(
		digest.MustNewDigest("freebsd", "7609128715518308672067aab169e24944ead24e3d732aab8a8f0b7013a65564", 5),
		true)
	require.NoError(t, err)
	require.Equal(t, ".", buildDirectoryPath)
	require.NoError(t, buildDirectory.Mkdir("world", os.FileMode(0700)))
	buildDirectory.Close()
}
