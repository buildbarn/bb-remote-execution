package filesystem_test

import (
	"io/ioutil"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	directory.EXPECT().IsWritable().Return(false, nil)
	directory.EXPECT().ReadDir().Return([]filesystem.FileInfo{}, nil)

	require.NoError(t, re_filesystem.CheckAllWritablePathsAreAllowed(directory, "/", nil))
}

func TestContainsWritableFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	directory.EXPECT().IsWritable().Return(false, nil)
	directory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo(path.MustNewComponent("writable"), filesystem.FileTypeRegularFile),
	}, nil)
	directory.EXPECT().IsWritableChild(path.MustNewComponent("writable")).Return(true, nil)

	require.Error(t, re_filesystem.CheckAllWritablePathsAreAllowed(directory, "/", nil))
}

func TestContainsWritableDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	directory.EXPECT().IsWritable().Return(false, nil)
	directory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo(path.MustNewComponent("writable"), filesystem.FileTypeDirectory),
	}, nil)
	subdir := mock.NewMockDirectoryCloser(ctrl)
	directory.EXPECT().EnterDirectory(path.MustNewComponent("writable")).Return(subdir, nil)
	subdir.EXPECT().IsWritable().Return(true, nil)
	subdir.EXPECT().Close()

	require.Error(t, re_filesystem.CheckAllWritablePathsAreAllowed(directory, "/", nil))
}

func TestContainsAllowedWritable(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	directory.EXPECT().IsWritable().Return(false, nil)
	directory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo(path.MustNewComponent("writable_dir"), filesystem.FileTypeDirectory),
		filesystem.NewFileInfo(path.MustNewComponent("writable_file"), filesystem.FileTypeRegularFile),
		filesystem.NewFileInfo(path.MustNewComponent("non_writable_file"), filesystem.FileTypeRegularFile),
		filesystem.NewFileInfo(path.MustNewComponent("readonly_dir"), filesystem.FileTypeDirectory),
		filesystem.NewFileInfo(path.MustNewComponent("symlink"), filesystem.FileTypeSymlink),
	}, nil)
	directory.EXPECT().IsWritableChild(path.MustNewComponent("non_writable_file")).Return(false, nil)

	subdir := mock.NewMockDirectoryCloser(ctrl)
	directory.EXPECT().EnterDirectory(path.MustNewComponent("readonly_dir")).Return(subdir, nil)
	subdir.EXPECT().IsWritable().Return(false, nil)
	subdir.EXPECT().ReadDir().Return([]filesystem.FileInfo{}, nil)
	subdir.EXPECT().Close()

	allowed := map[string]struct{}{
		"/writable_file": {},
		"/writable_dir":  {},
	}

	require.NoError(t, re_filesystem.CheckAllWritablePathsAreAllowed(directory, "/", allowed))
}

func TestWritableRootIsError(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "re_filesystem")
	require.NoError(t, err)
	directory, err := filesystem.NewLocalDirectory(tempdir)
	require.NoError(t, err)

	require.Error(t, re_filesystem.CheckAllWritablePathsAreAllowed(directory, tempdir, nil))
}
