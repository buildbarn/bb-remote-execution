package filesystem_test

import (
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
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
		underlyingDirectory.EXPECT().EnterDirectory(path.MustNewComponent("sub")).Return(childDirectory, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		returnedDirectory, err := directory.EnterDirectory(path.MustNewComponent("sub"))
		require.NoError(t, err)
		require.Equal(t, returnedDirectory, childDirectory)
	})

	t.Run("EnterFailure", func(t *testing.T) {
		directoryOpener.EXPECT().Call().Return(nil, status.Error(codes.PermissionDenied, "Not allowed to access build directory"))

		// Error code should be transformed to Internal. We
		// don't want to propagate the underlying error code, as
		// that could cause confusion/invalid behaviour (e.g.,
		// NotFound).
		_, err := directory.EnterDirectory(path.MustNewComponent("sub"))
		require.Equal(t, err, status.Error(codes.Internal, "Failed to open underlying directory: Not allowed to access build directory"))
	})

	t.Run("LinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		otherDirectory := mock.NewMockDirectoryCloser(ctrl)
		underlyingDirectory.EXPECT().Link(path.MustNewComponent("old"), otherDirectory, path.MustNewComponent("new")).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Link(path.MustNewComponent("old"), otherDirectory, path.MustNewComponent("new"))
		require.NoError(t, err)
	})

	t.Run("LstatSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Lstat(path.MustNewComponent("foo")).Return(filesystem.NewFileInfo(path.MustNewComponent("foo"), filesystem.FileTypeDirectory), nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		fileInfo, err := directory.Lstat(path.MustNewComponent("foo"))
		require.NoError(t, err)
		require.Equal(t, fileInfo, filesystem.NewFileInfo(path.MustNewComponent("foo"), filesystem.FileTypeDirectory))
	})

	t.Run("MkdirSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Mkdir(path.MustNewComponent("sub"), os.FileMode(0o777)).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Mkdir(path.MustNewComponent("sub"), 0o777)
		require.NoError(t, err)
	})

	t.Run("OpenReadSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		childFile := mock.NewMockFileReader(ctrl)
		underlyingDirectory.EXPECT().OpenRead(path.MustNewComponent("file")).Return(childFile, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		f, err := directory.OpenRead(path.MustNewComponent("file"))
		require.NoError(t, err)
		require.Equal(t, f, childFile)
	})

	t.Run("ReadDirSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("a"), filesystem.FileTypeDirectory),
			filesystem.NewFileInfo(path.MustNewComponent("b"), filesystem.FileTypeRegularFile),
		}, nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		contents, err := directory.ReadDir()
		require.NoError(t, err)
		require.Equal(t, contents, []filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("a"), filesystem.FileTypeDirectory),
			filesystem.NewFileInfo(path.MustNewComponent("b"), filesystem.FileTypeRegularFile),
		})
	})

	t.Run("ReadlinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Readlink(path.MustNewComponent("symlink")).Return("target", nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		target, err := directory.Readlink(path.MustNewComponent("symlink"))
		require.NoError(t, err)
		require.Equal(t, target, "target")
	})

	t.Run("RemoveSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Remove(path.MustNewComponent("file")).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Remove(path.MustNewComponent("file"))
		require.NoError(t, err)
	})

	t.Run("RemoveAllSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().RemoveAll(path.MustNewComponent("directory")).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.RemoveAll(path.MustNewComponent("directory"))
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
		underlyingDirectory.EXPECT().Rename(path.MustNewComponent("old"), otherDirectory, path.MustNewComponent("new"))
		underlyingDirectory.EXPECT().Close()

		require.NoError(t, directory.Rename(path.MustNewComponent("old"), otherDirectory, path.MustNewComponent("new")))
	})

	t.Run("SymlinkSuccess", func(t *testing.T) {
		underlyingDirectory := mock.NewMockDirectoryCloser(ctrl)
		directoryOpener.EXPECT().Call().Return(underlyingDirectory, nil)
		underlyingDirectory.EXPECT().Symlink("old", path.MustNewComponent("new")).Return(nil)
		underlyingDirectory.EXPECT().Close().Return(nil)

		// Call should be forwarded literally.
		err := directory.Symlink("old", path.MustNewComponent("new"))
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
