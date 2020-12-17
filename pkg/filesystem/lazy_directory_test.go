package filesystem_test

import (
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLazyDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	directoryOpener := mock.NewMockDirectoryOpener(ctrl)
	directory := re_filesystem.NewLazyDirectory(directoryOpener.Call)

	t.Run("EnterSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		childDirectory := mock.NewMockDirectoryCloser(ctrl)
		underlyingDirectory.EXPECT().EnterDirectory("sub").Return(childDirectory, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		returnedDirectory, err := directory.EnterDirectory("sub")
		require.NoError(t, err)
		require.Equal(t, returnedDirectory, childDirectory)
	})

	t.Run("EnterFailure", func(t *testing.T) {
		directoryOpener.EXPECT().Call().Return(nil, status.Error(codes.PermissionDenied, "Not allowed to access build directory"))

		// Error code should be transformed to Internal. We
		// don't want to propagate the underlying error code, as
		// that could cause confusion/invalid behaviour (e.g.,
		// NotFound).
		_, err := directory.EnterDirectory("sub")
		require.Equal(t, err, status.Error(codes.Internal, "Failed to open underlying directory: Not allowed to access build directory"))
	})

	t.Run("LinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		otherDirectory := mock.NewMockDirectoryCloser(ctrl)
		underlyingDirectory.EXPECT().Link("old", otherDirectory, "new").Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Link("old", otherDirectory, "new")
		require.NoError(t, err)
	})

	t.Run("LstatSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Lstat("foo").Return(filesystem.NewFileInfo("foo", filesystem.FileTypeDirectory), nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		fileInfo, err := directory.Lstat("foo")
		require.NoError(t, err)
		require.Equal(t, fileInfo, filesystem.NewFileInfo("foo", filesystem.FileTypeDirectory))
	})

	t.Run("MkdirSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Mkdir("sub", os.FileMode(0777)).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Mkdir("sub", 0777)
		require.NoError(t, err)
	})

	t.Run("OpenReadSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		childFile := mock.NewMockFileReader(ctrl)
		underlyingDirectory.EXPECT().OpenRead("file").Return(childFile, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		f, err := directory.OpenRead("file")
		require.NoError(t, err)
		require.Equal(t, f, childFile)
	})

	t.Run("ReadDirSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
			filesystem.NewFileInfo("a", filesystem.FileTypeDirectory),
			filesystem.NewFileInfo("b", filesystem.FileTypeRegularFile),
		}, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		contents, err := directory.ReadDir()
		require.NoError(t, err)
		require.Equal(t, contents, []filesystem.FileInfo{
			filesystem.NewFileInfo("a", filesystem.FileTypeDirectory),
			filesystem.NewFileInfo("b", filesystem.FileTypeRegularFile),
		})
	})

	t.Run("ReadlinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Readlink("symlink").Return("target", nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		target, err := directory.Readlink("symlink")
		require.NoError(t, err)
		require.Equal(t, target, "target")
	})

	t.Run("RemoveSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Remove("file").Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Remove("file")
		require.NoError(t, err)
	})

	t.Run("RemoveAllSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().RemoveAll("directory").Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.RemoveAll("directory")
		require.NoError(t, err)
	})

	t.Run("RemoveAllChildrenSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().RemoveAllChildren().Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.RemoveAllChildren()
		require.NoError(t, err)
	})

	t.Run("RenameSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		otherDirectory := mock.NewMockDirectoryCloser(ctrl)
		underlyingDirectory.EXPECT().Rename("old", otherDirectory, "new")
		underlyingDirectory.EXPECT().Close()

		require.NoError(t, directory.Rename("old", otherDirectory, "new"))
	})

	t.Run("SymlinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Symlink("old", "new").Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Symlink("old", "new")
		require.NoError(t, err)
	})

	t.Run("SyncSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Sync()
		underlyingDirectory.EXPECT().Close()

		require.NoError(t, directory.Sync())
	})
}
