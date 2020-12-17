// +build darwin linux

package fuse_test

import (
	"context"
	"os"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInMemoryDirectoryEnterBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Empty filename.
	_, err := d.EnterInMemoryDirectory("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	// Attempt to bypass directory hierarchy.
	_, err = d.EnterInMemoryDirectory(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.EnterInMemoryDirectory("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	// Skipping of intermediate directory levels.
	_, err = d.EnterInMemoryDirectory("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
}

func TestInMemoryDirectoryEnterNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	_, err := d.EnterInMemoryDirectory("nonexistent")
	require.True(t, os.IsNotExist(err))
}

func TestInMemoryDirectoryEnterFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))

	_, err := d.EnterInMemoryDirectory("file")
	require.Equal(t, syscall.ENOTDIR, err)
}

func TestInMemoryDirectoryEnterSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mkdir("subdir", 0777))
	_, err := d.EnterInMemoryDirectory("subdir")
	require.NoError(t, err)
}

func TestInMemoryDirectoryLstatBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	_, err := d.Lstat("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	_, err = d.Lstat(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.Lstat("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	_, err = d.Lstat("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
}

func TestInMemoryDirectoryLstatNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	_, err := d.Lstat("hello")
	require.True(t, os.IsNotExist(err))
}

func TestInMemoryDirectoryLstatFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))

	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	fi, err := d.Lstat("file")
	require.NoError(t, err)
	require.Equal(t, "file", fi.Name())
	require.Equal(t, filesystem.FileTypeRegularFile, fi.Type())
}

func TestInMemoryDirectoryLstatDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mkdir("directory", 0700))
	fi, err := d.Lstat("directory")
	require.NoError(t, err)
	require.Equal(t, "directory", fi.Name())
	require.Equal(t, filesystem.FileTypeDirectory, fi.Type())
}

func TestInMemoryDirectoryMkdirBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Mkdir("", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Mkdir(".", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Mkdir("..", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Mkdir("foo/bar", 0777))
}

func TestInMemoryDirectoryMkdirExisting(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))

	require.True(t, os.IsExist(d.Mkdir("file", 0777)))
}

func TestInMemoryDirectoryMkdirInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("directory", 0700))
	child, err := d.EnterInMemoryDirectory("directory")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "directory")
	require.NoError(t, d.RemoveAll("directory"))

	// Creating a directory inside of it should fail with ENOENT.
	require.True(t, os.IsNotExist(child.Mkdir("directory", 0777)))
}

func TestInMemoryDirectoryMkdirSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mkdir("directory", 0777))
}

func TestInMemoryDirectoryMknodBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.Equal(
		t,
		status.Error(codes.InvalidArgument, "Invalid filename: \"\""),
		d.Mknod("", os.ModeDevice|os.ModeCharDevice|0666, 123))
	require.Equal(
		t,
		status.Error(codes.InvalidArgument, "Invalid filename: \".\""),
		d.Mknod(".", os.ModeDevice|os.ModeCharDevice|0666, 123))
	require.Equal(
		t,
		status.Error(codes.InvalidArgument, "Invalid filename: \"..\""),
		d.Mknod("..", os.ModeDevice|os.ModeCharDevice|0666, 123))
	require.Equal(
		t,
		status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""),
		d.Mknod("foo/bar", os.ModeDevice|os.ModeCharDevice|0666, 123))
}

func TestInMemoryDirectoryMknodExisting(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"symlink": leaf}))

	require.True(t, os.IsExist(d.Mknod("symlink", os.ModeDevice|os.ModeCharDevice|0666, 259)))
}

func TestInMemoryDirectoryMknodSuccessCharacterDevice(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mknod("null", os.ModeDevice|os.ModeCharDevice|0666, 259))
	fi, err := d.Lstat("null")
	require.NoError(t, err)

	// Character device should simply be reported as 'other' when
	// requested through Lstat().
	require.Equal(t, filesystem.NewFileInfo("null", filesystem.FileTypeOther), fi)

	// When requested through FUSE, the provided device properties
	// should be returned properly.
	var attr go_fuse.Attr
	_, _, s := d.FUSELookup("null", &attr)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, attr, go_fuse.Attr{
		Mode:  syscall.S_IFCHR | 0666,
		Ino:   16619996191411179235,
		Nlink: fuse.StatelessLeafLinkCount,
		Rdev:  259,
	})
}

func TestInMemoryDirectoryReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Prepare file system.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))
	require.NoError(t, d.Mkdir("directory", 0777))

	// Validate directory listing.
	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	files, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, files,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("directory", filesystem.FileTypeDirectory),
			filesystem.NewFileInfo("file", filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryDirectoryReadlinkBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	_, err := d.Readlink("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	_, err = d.Readlink(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.Readlink("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	_, err = d.Readlink("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
}

func TestInMemoryDirectoryReadlinkNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	_, err := d.Readlink("nonexistent")
	require.True(t, os.IsNotExist(err))
}

func TestInMemoryDirectoryReadlinkDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mkdir("directory", 0777))
	_, err := d.Readlink("directory")
	require.Equal(t, syscall.EINVAL, err)
}

func TestInMemoryDirectoryReadlinkFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))

	leaf.EXPECT().Readlink().Return("", syscall.EINVAL)
	_, err := d.Readlink("file")
	require.Equal(t, syscall.EINVAL, err)
}

func TestInMemoryDirectoryReadlinkSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	var out go_fuse.Attr
	_, s := d.FUSESymlink("/foo/bar/baz", "symlink", &out)
	require.Equal(t, go_fuse.OK, s)

	target, err := d.Readlink("symlink")
	require.NoError(t, err)
	require.Equal(t, "/foo/bar/baz", target)
}

func TestInMemoryDirectoryRemoveBadName(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Remove(""))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Remove("."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Remove(".."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Remove("foo/bar"))
}

func TestInMemoryDirectoryRemoveNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.True(t, os.IsNotExist(d.Remove("nonexistent")))
}

func TestInMemoryDirectoryRemoveDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Test that removing a directory through filesystem.Directory
	// also triggers FUSE invalidations.
	require.NoError(t, d.Mkdir("directory", 0777))
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "directory")
	require.NoError(t, d.Remove("directory"))
}

func TestInMemoryDirectoryRemoveDirectoryNotEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	require.NoError(t, d.Mkdir("directory", 0777))
	child, err := d.EnterInMemoryDirectory("directory")
	require.NoError(t, err)
	require.NoError(t, child.Mkdir("subdirectory", 0777))
	require.Equal(t, syscall.ENOTEMPTY, d.Remove("directory"))
}

func TestInMemoryDirectoryRemoveFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))

	leaf.EXPECT().Unlink()
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "file")
	require.NoError(t, d.Remove("file"))
}

// TODO: Add testing coverage for RemoveAll().

func TestInMemoryDirectoryMergeDirectoryContentsSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Initial parent directory.
	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Merge another directory and file into it.
	subdirectoryFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	topLevelFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t,
		d.MergeDirectoryContents(
			map[string]fuse.InitialContentsFetcher{
				"dir": subdirectoryFetcher,
			},
			map[string]fuse.NativeLeaf{
				"file": topLevelFile,
			}))

	// Validate top-level directory listing.
	topLevelFile.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("dir", filesystem.FileTypeDirectory),
			filesystem.NewFileInfo("file", filesystem.FileTypeRegularFile),
		})

	// Validate subdirectory listing.
	subdirectory, err := d.EnterInMemoryDirectory("dir")
	require.NoError(t, err)
	subdirectoryFile := mock.NewMockNativeLeaf(ctrl)
	subdirectoryFetcher.EXPECT().FetchContents().Return(
		map[string]fuse.InitialContentsFetcher{},
		map[string]fuse.NativeLeaf{
			"file": subdirectoryFile,
		},
		nil)
	subdirectoryFile.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err = subdirectory.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("file", filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryDirectoryMergeDirectoryContentsInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("directory", 0700))
	child, err := d.EnterInMemoryDirectory("directory")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "directory")
	require.NoError(t, d.Remove("directory"))

	// Merging files into the removed directory should fail.
	require.Equal(t,
		child.MergeDirectoryContents(
			map[string]fuse.InitialContentsFetcher{},
			map[string]fuse.NativeLeaf{}),
		status.Error(codes.InvalidArgument, "Cannot merge contents into a directory that has already been deleted"))
}

func TestInMemoryDirectoryInstallHooks(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Initial top-level directory with custom hooks installed.
	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator1, errorLogger1, fuse.DeterministicInodeNumberTree, entryNotifier.Call)
	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	d.InstallHooks(fileAllocator2, errorLogger2)

	// Validate that the top-level directory uses both the new file
	// allocator and error logger.
	fileAllocator2.EXPECT().NewFile(uint64(16619996191411179235), os.FileMode(0644)).Return(nil, status.Error(codes.DataLoss, "Hard disk on fire"))
	errorLogger2.EXPECT().Log(status.Error(codes.DataLoss, "Failed to create new file with name \"foo\": Hard disk on fire"))
	var attr go_fuse.Attr
	_, s := d.FUSECreate("foo", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EIO))

	// Validate that a subdirectory uses the new file allocator
	// and error logger as well.
	require.NoError(t, d.Mkdir("dir", os.FileMode(0700)))
	_, err := d.EnterInMemoryDirectory("dir")
	require.NoError(t, err)
	fileAllocator2.EXPECT().NewFile(uint64(14113423708532309633), os.FileMode(0644)).Return(nil, status.Error(codes.DataLoss, "Hard disk on fire"))
	errorLogger2.EXPECT().Log(status.Error(codes.DataLoss, "Failed to create new file with name \"foo\": Hard disk on fire"))
	_, s = d.FUSECreate("foo", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EIO))
}

func TestInMemoryDirectoryUploadFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(
		map[string]fuse.InitialContentsFetcher{
			"directory": childDirectory,
		},
		map[string]fuse.NativeLeaf{
			"file": childFile,
		},
	))

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	childDigest := digest.MustNewDigest("example", "b834d64dfad425b9d7554febfdda5f33", 34)
	digestFunction := childDigest.GetDigestFunction()

	t.Run("FileSuccess", func(t *testing.T) {
		// Link() and Unlink() are called to ensure that
		// UploadFile() can be called without holding the
		// directory lock.
		gomock.InOrder(
			childFile.EXPECT().Link(),
			childFile.EXPECT().UploadFile(ctx, contentAddressableStorage, gomock.Any()).Return(childDigest, nil),
			childFile.EXPECT().Unlink())

		actualDigest, err := d.UploadFile(ctx, "file", contentAddressableStorage, digestFunction)
		require.NoError(t, err)
		require.Equal(t, childDigest, actualDigest)
	})

	t.Run("FileFailure", func(t *testing.T) {
		// Unlink() should still be performed if uploading fails.
		gomock.InOrder(
			childFile.EXPECT().Link(),
			childFile.EXPECT().UploadFile(ctx, contentAddressableStorage, gomock.Any()).Return(digest.BadDigest, status.Error(codes.Internal, "Server on fire")),
			childFile.EXPECT().Unlink())

		_, err := d.UploadFile(ctx, "file", contentAddressableStorage, digestFunction)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("Directory", func(t *testing.T) {
		_, err := d.UploadFile(ctx, "directory", contentAddressableStorage, digestFunction)
		require.Equal(t, syscall.EISDIR, err)
	})

	t.Run("Nonexistent", func(t *testing.T) {
		_, err := d.UploadFile(ctx, "nonexistent", contentAddressableStorage, digestFunction)
		require.Equal(t, syscall.ENOENT, err)
	})
}

func TestInMemoryDirectoryFUSECreateFileExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a file at the desired target location.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"target": leaf}))

	// Trying to create the file through FUSE should fail.
	var attr go_fuse.Attr
	_, s := d.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EEXIST))
}

func TestInMemoryDirectoryFUSECreateDirectoryExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a directory at the desired target location.
	err := d.Mkdir("target", 0777)
	require.NoError(t, err)

	// Trying to create the file through FUSE should fail.
	var attr go_fuse.Attr
	_, s := d.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EEXIST))
}

func TestInMemoryDirectoryFUSECreateAllocationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	fileAllocator.EXPECT().NewFile(uint64(16619996191411179235), os.FileMode(0644)).Return(nil, status.Error(codes.Internal, "Out of disk space"))
	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to create new file with name \"target\": Out of disk space"))
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// File allocation errors should translate to EIO. The actual
	// error should get forwarded to the error logger.
	var attr go_fuse.Attr
	_, s := d.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.EIO)
}

func TestInMemoryDirectoryFUSECreateOpenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	fileAllocator.EXPECT().NewFile(uint64(16619996191411179235), os.FileMode(0644)).Return(child, nil)
	child.EXPECT().FUSEOpen(uint32(os.O_WRONLY)).Return(go_fuse.EIO)
	child.EXPECT().Unlink()
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Creation should succeed, but failure to open should cause the
	// creation to be undone.
	var attr go_fuse.Attr
	_, s := d.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.EIO)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestInMemoryDirectoryFUSECreateInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("directory", 0700))
	child, err := d.EnterInMemoryDirectory("directory")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "directory")
	require.NoError(t, d.Remove("directory"))

	// Trying to create the file through FUSE should return ENOENT.
	var attr go_fuse.Attr
	_, s := child.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, s, go_fuse.ENOENT)
}

func TestInMemoryDirectoryFUSECreateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	fileAllocator.EXPECT().NewFile(uint64(16619996191411179235), os.FileMode(0644)).Return(child, nil)
	child.EXPECT().FUSEOpen(uint32(os.O_WRONLY)).Return(go_fuse.OK)
	child.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0644
		out.Ino = 2
	})
	child.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Creation of the directory should fully succeed. The file
	// should be present within the directory afterwards.
	var attr go_fuse.Attr
	newChild, s := d.FUSECreate("target", uint32(os.O_WRONLY), 0644, &attr)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, child, newChild)
	require.Equal(t, go_fuse.Attr{
		Mode: go_fuse.S_IFREG | 0644,
		Ino:  2,
	}, attr)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("target", filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryDirectoryFUSEGetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// An empty directory should have link count 2.
	var attr1 go_fuse.Attr
	d.FUSEGetAttr(&attr1)
	require.Equal(t, attr1, go_fuse.Attr{
		Mode:  go_fuse.S_IFDIR | 0777,
		Ino:   578437695752307201,
		Nlink: 2,
	})

	// Creating non-directory nodes within the directory should not
	// cause the link count to be increased.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"target": leaf}))
	var attr2 go_fuse.Attr
	_, s := d.FUSESymlink("/", "symlink", &attr2)
	require.Equal(t, go_fuse.OK, s)

	var attr3 go_fuse.Attr
	d.FUSEGetAttr(&attr3)
	require.Equal(t, attr3, go_fuse.Attr{
		Mode:  go_fuse.S_IFDIR | 0777,
		Ino:   578437695752307201,
		Nlink: 2,
	})

	// Creating child directories should increase the link count.
	require.NoError(t, d.Mkdir("dir1", 0777))
	require.NoError(t, d.Mkdir("dir2", 0777))
	var attr4 go_fuse.Attr
	d.FUSEGetAttr(&attr4)
	require.Equal(t, attr4, go_fuse.Attr{
		Mode:  go_fuse.S_IFDIR | 0777,
		Ino:   578437695752307201,
		Nlink: 4,
	})
}

func TestInMemoryDirectoryFUSELinkExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Attempting to link to a file that already exists should fail.
	require.NoError(t, d.Mkdir("dir", 0777))
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.Status(syscall.EEXIST), d.FUSELink("dir", child, &attr))
}

func TestInMemoryDirectoryFUSELinkInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	target := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("directory", 0700))
	child, err := d.EnterInMemoryDirectory("directory")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "directory")
	require.NoError(t, d.Remove("directory"))

	// Linking a file into it should fail with ENOENT.
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.ENOENT, child.FUSELink("target", target, &attr))
}

func TestInMemoryDirectoryFUSELinkSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	child.EXPECT().Link()
	child.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0644
		out.Ino = 123
	})
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// We should return the attributes of the existing leaf.
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.OK, d.FUSELink("target", child, &attr))
	require.Equal(t, go_fuse.Attr{
		Mode: go_fuse.S_IFREG | 0644,
		Ino:  123,
	}, attr)
}

func TestInMemoryDirectoryFUSELookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create an example directory and file that we'll try to look up.
	require.NoError(t, d.Mkdir("dir", 0777))
	directory, err := d.EnterInMemoryDirectory("dir")
	require.NoError(t, err)

	file := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": file}))

	t.Run("NotFound", func(*testing.T) {
		var attr go_fuse.Attr
		_, _, s := d.FUSELookup("missing", &attr)
		require.Equal(t, go_fuse.ENOENT, s)
	})

	t.Run("FoundDirectory", func(*testing.T) {
		var attr go_fuse.Attr
		newDirectory, newLeaf, s := d.FUSELookup("dir", &attr)
		require.Equal(t, go_fuse.OK, s)
		require.Equal(t, directory, newDirectory)
		require.Nil(t, newLeaf)
		require.Equal(t, go_fuse.Attr{
			Mode:  go_fuse.S_IFDIR | 0777,
			Ino:   16619996191411179235,
			Nlink: 2,
		}, attr)
	})

	t.Run("FoundFile", func(*testing.T) {
		file.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
			out.Mode = go_fuse.S_IFREG | 0666
			out.Ino = 3
			out.Size = 123
			out.Nlink = 1
		})

		var attr go_fuse.Attr
		newDirectory, newLeaf, s := d.FUSELookup("file", &attr)
		require.Equal(t, go_fuse.OK, s)
		require.Nil(t, newDirectory)
		require.Equal(t, file, newLeaf)
		require.Equal(t, go_fuse.Attr{
			Mode:  go_fuse.S_IFREG | 0666,
			Ino:   3,
			Size:  123,
			Nlink: 1,
		}, attr)
	})
}

func TestInMemoryDirectoryFUSEMknodExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Files may not be overwritten by mknod().
	require.NoError(t, d.Mkdir("dir", 0777))
	var attr go_fuse.Attr
	_, s := d.FUSEMknod("dir", go_fuse.S_IFIFO|0666, 0, &attr)
	require.Equal(t, go_fuse.Status(syscall.EEXIST), s)
}

func TestInMemoryDirectoryFUSEMknodPermissionDenied(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// This implementation should not allow the creation of block
	// devices and character devices.
	var attr go_fuse.Attr
	_, s := d.FUSEMknod("blk", syscall.S_IFBLK|0666, 123, &attr)
	require.Equal(t, go_fuse.EPERM, s)
	_, s = d.FUSEMknod("chr", syscall.S_IFCHR|0666, 123, &attr)
	require.Equal(t, go_fuse.EPERM, s)
}

func TestInMemoryDirectoryFUSEMknodSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a FIFO and a UNIX domain socket.
	var fifoAttr go_fuse.Attr
	i, s := d.FUSEMknod("fifo", go_fuse.S_IFIFO|0666, 123, &fifoAttr)
	require.Equal(t, go_fuse.OK, s)
	require.NotNil(t, i)
	require.Equal(t, go_fuse.Attr{
		Mode:  go_fuse.S_IFIFO | 0666,
		Ino:   16619996191411179235,
		Nlink: fuse.StatelessLeafLinkCount,
		Rdev:  123,
	}, fifoAttr)

	var socketAttr go_fuse.Attr
	i, s = d.FUSEMknod("socket", syscall.S_IFSOCK|0666, 456, &socketAttr)
	require.Equal(t, go_fuse.OK, s)
	require.NotNil(t, i)
	require.Equal(t, go_fuse.Attr{
		Mode:  syscall.S_IFSOCK | 0666,
		Ino:   2941613486566876440,
		Nlink: fuse.StatelessLeafLinkCount,
		Rdev:  456,
	}, socketAttr)

	// Buildbarn itself doesn't really care about these file types,
	// so they should simply be reported as 'Other'.
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("fifo", filesystem.FileTypeOther),
			filesystem.NewFileInfo("socket", filesystem.FileTypeOther),
		}, entries)

	// The FUSE file system should report the proper file types.
	fuseEntries, s := d.FUSEReadDir()
	require.Equal(t, go_fuse.OK, s)
	require.ElementsMatch(t, []go_fuse.DirEntry{
		{Name: "fifo", Mode: go_fuse.S_IFIFO, Ino: 16619996191411179235},
		{Name: "socket", Mode: syscall.S_IFSOCK, Ino: 2941613486566876440},
	}, fuseEntries)
}

func TestInMemoryDirectoryFUSEReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(
		map[string]fuse.InitialContentsFetcher{
			"directory": childDirectory,
		},
		map[string]fuse.NativeLeaf{
			"file": childFile,
		},
	))

	// Obtaining the directory listing through FUSEReadDir() should
	// not cause the child directory to be initialized. We don't
	// depend on any of its properties to populate its DirEntry, nor
	// are we returning a handle to it. A successive FUSELookup()
	// call will initialize the directory.
	childFile.EXPECT().FUSEGetDirEntry().Return(go_fuse.DirEntry{
		Mode: go_fuse.S_IFREG,
		Ino:  123,
	})
	fuseEntries, s := d.FUSEReadDir()
	require.Equal(t, go_fuse.OK, s)
	require.ElementsMatch(t, []go_fuse.DirEntry{
		{Name: "directory", Mode: go_fuse.S_IFDIR, Ino: 16619996191411179235},
		{Name: "file", Mode: syscall.S_IFREG, Ino: 123},
	}, fuseEntries)
}

func TestInMemoryDirectoryFUSEReadDirPlus(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(
		map[string]fuse.InitialContentsFetcher{
			"directory": childDirectory,
		},
		map[string]fuse.NativeLeaf{
			"file": childFile,
		},
	))

	childFile.EXPECT().FUSEGetDirEntry().Return(go_fuse.DirEntry{
		Mode: go_fuse.S_IFREG,
		Ino:  123,
	}).AnyTimes()

	t.Run("Failure", func(t *testing.T) {
		// FUSEReadDirPlus() returns handles to subdirectories,
		// meaning that subdirectories need to be initialized.
		// Simulate an initialization failure.
		childDirectory.EXPECT().FetchContents().
			Return(nil, nil, status.Error(codes.Unavailable, "Storage on fire"))
		errorLogger.EXPECT().Log(
			status.Error(
				codes.Unavailable,
				"Failed to initialize directory \"directory\" during readdir: Storage on fire"))

		_, _, s := d.FUSEReadDirPlus()
		require.Equal(t, go_fuse.EIO, s)
	})

	t.Run("Success", func(t *testing.T) {
		// Test the case where initialization of the
		// subdirectory succeeds.
		subDirectory := mock.NewMockInitialContentsFetcher(ctrl)
		subFile := mock.NewMockNativeLeaf(ctrl)
		childDirectory.EXPECT().FetchContents().Return(
			map[string]fuse.InitialContentsFetcher{
				"directory": subDirectory,
			},
			map[string]fuse.NativeLeaf{
				"file": subFile,
			},
			nil)

		directories, leaves, s := d.FUSEReadDirPlus()
		require.Equal(t, go_fuse.OK, s)
		require.Len(t, directories, 1)
		require.Equal(t, go_fuse.DirEntry{
			Name: "directory",
			Mode: go_fuse.S_IFDIR,
			Ino:  16619996191411179235,
		}, directories[0].DirEntry)
		require.ElementsMatch(t, []fuse.LeafDirEntry{
			{
				Child: subFile,
				DirEntry: go_fuse.DirEntry{
					Name: "file",
					Mode: go_fuse.S_IFREG,
					Ino:  123,
				},
			},
		}, leaves)

		// The directory handles returned by FUSEReadDirPlus()
		// should be actually usable. Check that the link count
		// is three, indicating that subDirectory is placed
		// inside.
		var attr go_fuse.Attr
		directories[0].Child.FUSEGetAttr(&attr)
		require.Equal(t, attr, go_fuse.Attr{
			Mode:  go_fuse.S_IFDIR | 0777,
			Ino:   16619996191411179235,
			Nlink: 3,
		})
	})
}

func TestInMemoryDirectoryFUSERenameSelfDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Renaming a directory to itself should be permitted, even when
	// it is not empty.
	require.NoError(t, d.Mkdir("dir", 0777))
	child, err := d.EnterInMemoryDirectory("dir")
	require.NoError(t, err)
	require.NoError(t, child.Mkdir("subdir", 0777))
	require.Equal(t, go_fuse.OK, d.FUSERename("dir", d, "dir"))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("dir", filesystem.FileTypeDirectory),
		})
}

func TestInMemoryDirectoryFUSERenameSelfFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"a": leaf}))

	leaf.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0666
		out.Ino = 3
		out.Size = 123
		out.Nlink = 1
	})
	var out go_fuse.Attr
	leaf.EXPECT().Link()
	require.Equal(t, go_fuse.OK, d.FUSELink("b", leaf, &out))

	// Renaming a file to itself should have no effect. This even
	// applies to hard links. Though not intuitive, this means that
	// the source file may continue to exist.
	require.Equal(t, go_fuse.OK, d.FUSERename("a", d, "b"))

	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("a", filesystem.FileTypeRegularFile),
			filesystem.NewFileInfo("b", filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryDirectoryFUSERenameDirectoryInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("removed", 0700))
	child, err := d.EnterInMemoryDirectory("removed")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "removed")
	require.NoError(t, d.Remove("removed"))

	// Moving a directory into it should fail with ENOENT.
	require.NoError(t, d.Mkdir("dir", 0777))
	require.Equal(t, go_fuse.ENOENT, d.FUSERename("dir", child, "dir"))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("dir", filesystem.FileTypeDirectory),
		})
}

func TestInMemoryDirectoryFUSERenameFileInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create a reference to a removed child directory.
	require.NoError(t, d.Mkdir("removed", 0700))
	child, err := d.EnterInMemoryDirectory("removed")
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(578437695752307201), "removed")
	require.NoError(t, d.Remove("removed"))

	// Moving a file into it should fail with ENOENT.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.MergeDirectoryContents(nil, map[string]fuse.NativeLeaf{"file": leaf}))
	require.Equal(t, go_fuse.ENOENT, d.FUSERename("file", child, "file"))

	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("file", filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryDirectoryFUSERenameDirectoryTwice(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	// Create two empty directories.
	require.NoError(t, d.Mkdir("a", 0700))
	childA, err := d.EnterInMemoryDirectory("a")
	require.NoError(t, err)
	require.NoError(t, d.Mkdir("b", 0700))
	childB, err := d.EnterInMemoryDirectory("b")
	require.NoError(t, err)

	// Move "a" to "b" to "c". Afterwards, only "c" should remain.
	require.Equal(t, go_fuse.OK, d.FUSERename("a", d, "b"))
	require.Equal(t, go_fuse.OK, d.FUSERename("b", d, "c"))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("c", filesystem.FileTypeDirectory),
		})

	// Directory "a" got moved over "b", meaning that only the
	// former should still be usable. The latter has been deleted.
	require.NoError(t, childA.Mkdir("subdirectory", 0700))
	require.Equal(t, syscall.ENOENT, childB.Mkdir("subdirectory", 0700))
}

func TestInMemoryDirectoryFUSERenameCrossDevice1(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d1 := fuse.NewInMemoryDirectory(fileAllocator, errorLogger, fuse.DeterministicInodeNumberTree, entryNotifier.Call)

	d2 := mock.NewMockFUSEDirectory(ctrl)

	// Attempting to rename a file to a directory that is of a
	// completely different type is not possible. We can only rename
	// objects between instances of InMemoryDirectory.
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename("src", d2, "dst"))
}

func TestInMemoryDirectoryFUSERenameCrossDevice2(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	entryNotifier1 := mock.NewMockEntryNotifier(ctrl)
	d1 := fuse.NewInMemoryDirectory(fileAllocator1, errorLogger1, fuse.DeterministicInodeNumberTree, entryNotifier1.Call)

	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	entryNotifier2 := mock.NewMockEntryNotifier(ctrl)
	d2 := fuse.NewInMemoryDirectory(fileAllocator2, errorLogger2, fuse.DeterministicInodeNumberTree, entryNotifier2.Call)

	// It should not be possible to rename directories from one
	// hierarchy to another, as this completely messes up
	// InMemoryDirectory's internal bookkeeping.
	require.NoError(t, d1.Mkdir("src", 0700))
	require.NoError(t, d2.Mkdir("dst", 0700))
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename("src", d2, "dst"))
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename("src", d2, "nonexistent"))

	// Renaming files leaf files between directory hierarchies is
	// completely safe. It's generally not useful to do this, but
	// even if we disallowed this explicitly, it would still be
	// possible to achieve this by hardlinking.
	require.NoError(t, d1.Mknod("leaf", os.ModeDevice|os.ModeCharDevice|0666, 123))
	require.Equal(t, go_fuse.OK, d1.FUSERename("leaf", d2, "leaf"))
}

// TODO: Missing testing coverage for FUSEMkdir(), FUSEReadDir(),
// FUSERename(), FUSERmdir(), FUSESymlink() and FUSEUnlink().
