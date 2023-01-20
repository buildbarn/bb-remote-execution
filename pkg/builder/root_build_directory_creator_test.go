package builder_test

import (
	"context"
	"os"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRootBuildDirectoryCreator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	mockBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator := builder.NewRootBuildDirectoryCreator(mockBuildDirectory)

	// Run a simple build action that only performs an Mkdir() call.
	// Once terminated, the underlying build directory should not be
	// closed, as it is reused by the next build action.
	mockBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("hello"), os.FileMode(0o700))
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(
		ctx,
		digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
		true)
	require.NoError(t, err)
	require.Nil(t, buildDirectoryPath)
	require.NoError(t, buildDirectory.Mkdir(path.MustNewComponent("hello"), os.FileMode(0o700)))
	buildDirectory.Close()

	// Run an action similar to the previous one. It should be
	// applied against the same underlying build directory.
	mockBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("world"), os.FileMode(0o700))
	buildDirectory, buildDirectoryPath, err = buildDirectoryCreator.GetBuildDirectory(
		ctx,
		digest.MustNewDigest("freebsd", remoteexecution.DigestFunction_SHA256, "7609128715518308672067aab169e24944ead24e3d732aab8a8f0b7013a65564", 5),
		true)
	require.NoError(t, err)
	require.Nil(t, buildDirectoryPath)
	require.NoError(t, buildDirectory.Mkdir(path.MustNewComponent("world"), os.FileMode(0o700)))
	buildDirectory.Close()
}
