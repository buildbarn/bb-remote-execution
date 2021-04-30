// +build darwin linux

package fuse_test

import (
	"os"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInMemoryPrepopulatedDirectoryLookupChildNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	_, _, err := d.LookupChild(path.MustNewComponent("nonexistent"))
	require.True(t, os.IsNotExist(err))
}

func TestInMemoryPrepopulatedDirectoryLookupChildFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("file"): {
			Leaf: leaf,
		},
	}, false))

	childDirectory, childLeaf, err := d.LookupChild(path.MustNewComponent("file"))
	require.NoError(t, err)
	require.Nil(t, childDirectory)
	require.Equal(t, leaf, childLeaf)
}

func TestInMemoryPrepopulatedDirectoryLookupChildDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("subdir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))

	childDirectory, childLeaf, err := d.LookupChild(path.MustNewComponent("subdir"))
	require.NoError(t, err)
	require.NotNil(t, childDirectory)
	require.Nil(t, childLeaf)
}

func TestInMemoryPrepopulatedDirectoryLookupAllChildrenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("subdir"): {
			Directory: initialContentsFetcher,
		},
	}, false))

	childDirectory, childLeaf, err := d.LookupChild(path.MustNewComponent("subdir"))
	require.NoError(t, err)
	require.NotNil(t, childDirectory)
	require.Nil(t, childLeaf)

	// When LookupAllChildren() is called in an uninitialized
	// directory and initialization fails, the error should be
	// propagated to the caller.
	initialContentsFetcher.EXPECT().FetchContents().
		Return(nil, status.Error(codes.Internal, "Network error"))
	_, _, err = childDirectory.LookupAllChildren()
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Network error"), err)
}

func TestInMemoryPrepopulatedDirectoryLookupAllChildrenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Populate the directory with files and directories.
	leaf1 := mock.NewMockNativeLeaf(ctrl)
	leaf2 := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("leaf1"): {
			Leaf: leaf1,
		},
		path.MustNewComponent("leaf2"): {
			Leaf: leaf2,
		},
	}, false))

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	subdir1, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("subdir1"))
	require.NoError(t, err)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	subdir2, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("subdir2"))
	require.NoError(t, err)

	// All children should be returned in sorted order.
	directories, leaves, err := d.LookupAllChildren()
	require.NoError(t, err)
	require.Equal(t, []fuse.DirectoryPrepopulatedDirEntry{
		{Name: path.MustNewComponent("subdir1"), Child: subdir1},
		{Name: path.MustNewComponent("subdir2"), Child: subdir2},
	}, directories)
	require.Equal(t, []fuse.LeafPrepopulatedDirEntry{
		{Name: path.MustNewComponent("leaf1"), Child: leaf1},
		{Name: path.MustNewComponent("leaf2"), Child: leaf2},
	}, leaves)
}

func TestInMemoryPrepopulatedDirectoryReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Prepare file system.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
		path.MustNewComponent("file"): {
			Leaf: leaf,
		},
	}, false))

	// Validate directory listing.
	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	files, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, files,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("directory"), filesystem.FileTypeDirectory),
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryPrepopulatedDirectoryRemoveNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	require.True(t, os.IsNotExist(d.Remove(path.MustNewComponent("nonexistent"))))
}

func TestInMemoryPrepopulatedDirectoryRemoveDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))

	// Test that removing a directory through filesystem.Directory
	// also triggers FUSE invalidations.
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("directory"))
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))
}

func TestInMemoryPrepopulatedDirectoryRemoveDirectoryNotEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory"): {
			Directory: initialContentsFetcher,
		},
	}, false))
	leaf := mock.NewMockNativeLeaf(ctrl)
	initialContentsFetcher.EXPECT().FetchContents().Return(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("file"): {
			Leaf: leaf,
		},
	}, nil)

	require.Equal(t, syscall.ENOTEMPTY, d.Remove(path.MustNewComponent("directory")))
}

func TestInMemoryPrepopulatedDirectoryRemoveFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("file"): {
			Leaf: leaf,
		},
	}, false))

	leaf.EXPECT().Unlink()
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("file"))
	require.NoError(t, d.Remove(path.MustNewComponent("file")))
}

// TODO: Add testing coverage for RemoveAll().

func TestInMemoryPrepopulatedDirectoryCreateChildrenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Initial parent directory.
	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Merge another directory and file into it.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	subdirectoryFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	topLevelFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dir"): {
			Directory: subdirectoryFetcher,
		},
		path.MustNewComponent("file"): {
			Leaf: topLevelFile,
		},
	}, false))

	// Validate top-level directory listing.
	topLevelFile.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory),
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile),
		})

	// Validate subdirectory listing.
	subdirectory, _, err := d.LookupChild(path.MustNewComponent("dir"))
	require.NoError(t, err)
	subdirectoryFile := mock.NewMockNativeLeaf(ctrl)
	subdirectoryFetcher.EXPECT().FetchContents().Return(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("file"): {
			Leaf: subdirectoryFile,
		},
	}, nil)
	subdirectoryFile.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err = subdirectory.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryPrepopulatedDirectoryCreateChildrenInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a reference to a removed child directory.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("directory"))
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Merging files into the removed directory should fail.
	require.Equal(t, syscall.ENOENT, child.CreateChildren(map[path.Component]fuse.InitialNode{}, false))
}

func TestInMemoryPrepopulatedDirectoryInstallHooks(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Initial top-level directory with custom hooks installed.
	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator1, errorLogger1, 100, inodeNumberGenerator, entryNotifier.Call)
	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	d.InstallHooks(fileAllocator2, errorLogger2)

	// Validate that the top-level directory uses both the new file
	// allocator and error logger.
	fileAllocator2.EXPECT().NewFile(uint32(os.O_WRONLY), uint32(0o644)).Return(nil, go_fuse.EIO)
	var attr go_fuse.Attr
	_, s := d.FUSECreate(path.MustNewComponent("foo"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EIO))

	// Validate that a subdirectory uses the new file allocator
	// and error logger as well.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("dir"))
	require.NoError(t, err)
	fileAllocator2.EXPECT().NewFile(uint32(os.O_WRONLY), uint32(0o644)).Return(nil, go_fuse.EIO)
	_, s = child.FUSECreate(path.MustNewComponent("foo"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EIO))
}

func TestInMemoryPrepopulatedDirectoryFilterChildren(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// In the initial state, InMemoryPrepopulatedDirectory will have
	// an EmptyInitialContentsFetcher associated with it.
	childFilter1 := mock.NewMockChildFilter(ctrl)
	childFilter1.EXPECT().Call(fuse.InitialNode{Directory: fuse.EmptyInitialContentsFetcher}, gomock.Any()).Return(true)
	require.NoError(t, d.FilterChildren(childFilter1.Call))

	// After attempting to access the directory's contents, the
	// InitialContentsFetcher should be evaluated. Successive
	// FilterChildren() calls will no longer report it.
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Empty(t, entries)

	childFilter2 := mock.NewMockChildFilter(ctrl)
	require.NoError(t, d.FilterChildren(childFilter2.Call))

	// Create some children and call FilterChildren() again. All
	// children should be reported. Remove some of them.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	directory1 := mock.NewMockInitialContentsFetcher(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	directory2 := mock.NewMockInitialContentsFetcher(ctrl)
	leaf1 := mock.NewMockNativeLeaf(ctrl)
	leaf2 := fuse.NewSymlink("target")
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory1"): {Directory: directory1},
		path.MustNewComponent("directory2"): {Directory: directory2},
		path.MustNewComponent("leaf1"):      {Leaf: leaf1},
		path.MustNewComponent("leaf2"):      {Leaf: leaf2},
	}, false))

	childFilter3 := mock.NewMockChildFilter(ctrl)
	childFilter3.EXPECT().Call(fuse.InitialNode{Directory: directory1}, gomock.Any()).
		DoAndReturn(func(initialNode fuse.InitialNode, remove func() error) bool {
			require.NoError(t, remove())
			return true
		})
	childFilter3.EXPECT().Call(fuse.InitialNode{Directory: directory2}, gomock.Any()).Return(true)
	childFilter3.EXPECT().Call(fuse.InitialNode{Leaf: leaf1}, gomock.Any()).
		DoAndReturn(func(initialNode fuse.InitialNode, remove func() error) bool {
			leaf1.EXPECT().Unlink()
			entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("leaf1"))
			require.NoError(t, remove())
			return true
		})
	childFilter3.EXPECT().Call(fuse.InitialNode{Leaf: leaf2}, gomock.Any()).Return(true)
	require.NoError(t, d.FilterChildren(childFilter3.Call))

	// Another call to FilterChildren() should only report the
	// children that were not removed previously.
	childFilter4 := mock.NewMockChildFilter(ctrl)
	childFilter4.EXPECT().Call(fuse.InitialNode{Directory: directory2}, gomock.Any()).Return(true)
	childFilter4.EXPECT().Call(fuse.InitialNode{Leaf: leaf2}, gomock.Any()).Return(true)
	require.NoError(t, d.FilterChildren(childFilter4.Call))
}

func TestInMemoryPrepopulatedDirectoryFUSECreateFileExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a file at the desired target location.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("target"): {
			Leaf: leaf,
		},
	}, false))

	// Trying to create the file through FUSE should fail.
	var attr go_fuse.Attr
	_, s := d.FUSECreate(path.MustNewComponent("target"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EEXIST))
}

func TestInMemoryPrepopulatedDirectoryFUSECreateDirectoryExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a directory at the desired target location.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("target"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))

	// Trying to create the file through FUSE should fail.
	var attr go_fuse.Attr
	_, s := d.FUSECreate(path.MustNewComponent("target"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.Status(syscall.EEXIST))
}

func TestInMemoryPrepopulatedDirectoryFUSECreateAllocationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	fileAllocator.EXPECT().NewFile(uint32(os.O_WRONLY), uint32(0o644)).Return(nil, go_fuse.EIO)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// File allocation errors should translate to EIO. The actual
	// error should get forwarded to the error logger.
	var attr go_fuse.Attr
	_, s := d.FUSECreate(path.MustNewComponent("target"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.EIO)
}

func TestInMemoryPrepopulatedDirectoryFUSECreateInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a reference to a removed child directory.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("directory"))
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Trying to create the file through FUSE should return ENOENT.
	var attr go_fuse.Attr
	_, s := child.FUSECreate(path.MustNewComponent("target"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, s, go_fuse.ENOENT)
}

func TestInMemoryPrepopulatedDirectoryFUSECreateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	fileAllocator.EXPECT().NewFile(uint32(os.O_WRONLY), uint32(0o644)).Return(child, go_fuse.OK)
	child.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0o644
		out.Ino = 2
	})
	child.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Creation of the directory should fully succeed. The file
	// should be present within the directory afterwards.
	var attr go_fuse.Attr
	newChild, s := d.FUSECreate(path.MustNewComponent("target"), uint32(os.O_WRONLY), 0o644, &attr)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, child, newChild)
	require.Equal(t, go_fuse.Attr{
		Mode: go_fuse.S_IFREG | 0o644,
		Ino:  2,
	}, attr)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("target"), filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryPrepopulatedDirectoryFUSEGetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	var attr1 go_fuse.Attr
	d.FUSEGetAttr(&attr1)
	require.Equal(t, attr1, go_fuse.Attr{
		Mode:  go_fuse.S_IFDIR | 0o777,
		Ino:   100,
		Nlink: fuse.ImplicitDirectoryLinkCount,
	})
}

func TestInMemoryPrepopulatedDirectoryFUSELinkExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Attempting to link to a file that already exists should fail.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.Status(syscall.EEXIST), d.FUSELink(path.MustNewComponent("dir"), child, &attr))
}

func TestInMemoryPrepopulatedDirectoryFUSELinkInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	target := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a reference to a removed child directory.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("directory"))
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Linking a file into it should fail with ENOENT.
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.ENOENT, child.FUSELink(path.MustNewComponent("target"), target, &attr))
}

func TestInMemoryPrepopulatedDirectoryFUSELinkNotNativeLeaf(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Trying to link a file that does not implement NativeLeaf is
	// not possible. We can only store leaf nodes that implement
	// this interface.
	child := mock.NewMockFUSELeaf(ctrl)
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.EXDEV, d.FUSELink(path.MustNewComponent("target"), child, &attr))
}

func TestInMemoryPrepopulatedDirectoryFUSELinkSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	child.EXPECT().Link()
	child.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0o644
		out.Ino = 123
	})
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// We should return the attributes of the existing leaf.
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.OK, d.FUSELink(path.MustNewComponent("target"), child, &attr))
	require.Equal(t, go_fuse.Attr{
		Mode: go_fuse.S_IFREG | 0o644,
		Ino:  123,
	}, attr)
}

func TestInMemoryPrepopulatedDirectoryFUSELookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create an example directory and file that we'll try to look up.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	file := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
		path.MustNewComponent("file"): {
			Leaf: file,
		},
	}, false))

	t.Run("NotFound", func(*testing.T) {
		var attr go_fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("missing"), &attr)
		require.Equal(t, go_fuse.ENOENT, s)
	})

	t.Run("FoundDirectory", func(*testing.T) {
		var attr go_fuse.Attr
		newDirectory, newLeaf, s := d.FUSELookup(path.MustNewComponent("dir"), &attr)
		require.Equal(t, go_fuse.OK, s)
		require.NotNil(t, newDirectory)
		require.Nil(t, newLeaf)
		require.Equal(t, go_fuse.Attr{
			Mode:  go_fuse.S_IFDIR | 0o777,
			Ino:   101,
			Nlink: fuse.ImplicitDirectoryLinkCount,
		}, attr)
	})

	t.Run("FoundFile", func(*testing.T) {
		file.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
			out.Mode = go_fuse.S_IFREG | 0o666
			out.Ino = 3
			out.Size = 123
			out.Nlink = 1
		})

		var attr go_fuse.Attr
		newDirectory, newLeaf, s := d.FUSELookup(path.MustNewComponent("file"), &attr)
		require.Equal(t, go_fuse.OK, s)
		require.Nil(t, newDirectory)
		require.Equal(t, file, newLeaf)
		require.Equal(t, go_fuse.Attr{
			Mode:  go_fuse.S_IFREG | 0o666,
			Ino:   3,
			Size:  123,
			Nlink: 1,
		}, attr)
	})
}

func TestInMemoryPrepopulatedDirectoryFUSEMknodExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Files may not be overwritten by mknod().
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	var attr go_fuse.Attr
	_, s := d.FUSEMknod(path.MustNewComponent("dir"), go_fuse.S_IFIFO|0o666, 0, &attr)
	require.Equal(t, go_fuse.Status(syscall.EEXIST), s)
}

func TestInMemoryPrepopulatedDirectoryFUSEMknodPermissionDenied(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// This implementation should not allow the creation of block
	// devices and character devices.
	var attr go_fuse.Attr
	_, s := d.FUSEMknod(path.MustNewComponent("blk"), syscall.S_IFBLK|0o666, 123, &attr)
	require.Equal(t, go_fuse.EPERM, s)
	_, s = d.FUSEMknod(path.MustNewComponent("chr"), syscall.S_IFCHR|0o666, 123, &attr)
	require.Equal(t, go_fuse.EPERM, s)
}

func TestInMemoryPrepopulatedDirectoryFUSEMknodSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a FIFO and a UNIX domain socket.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	var fifoAttr go_fuse.Attr
	i, s := d.FUSEMknod(path.MustNewComponent("fifo"), go_fuse.S_IFIFO|0o666, 123, &fifoAttr)
	require.Equal(t, go_fuse.OK, s)
	require.NotNil(t, i)
	require.Equal(t, go_fuse.Attr{
		Mode:  go_fuse.S_IFIFO | 0o666,
		Ino:   101,
		Nlink: fuse.StatelessLeafLinkCount,
		Rdev:  123,
	}, fifoAttr)

	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	var socketAttr go_fuse.Attr
	i, s = d.FUSEMknod(path.MustNewComponent("socket"), syscall.S_IFSOCK|0o666, 456, &socketAttr)
	require.Equal(t, go_fuse.OK, s)
	require.NotNil(t, i)
	require.Equal(t, go_fuse.Attr{
		Mode:  syscall.S_IFSOCK | 0o666,
		Ino:   102,
		Nlink: fuse.StatelessLeafLinkCount,
		Rdev:  456,
	}, socketAttr)

	// Buildbarn itself doesn't really care about these file types,
	// so they should simply be reported as 'Other'.
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("fifo"), filesystem.FileTypeOther),
			filesystem.NewFileInfo(path.MustNewComponent("socket"), filesystem.FileTypeOther),
		}, entries)

	// The FUSE file system should report the proper file types.
	fuseEntries, s := d.FUSEReadDir()
	require.Equal(t, go_fuse.OK, s)
	require.ElementsMatch(t, []go_fuse.DirEntry{
		{Name: "fifo", Mode: go_fuse.S_IFIFO, Ino: 101},
		{Name: "socket", Mode: syscall.S_IFSOCK, Ino: 102},
	}, fuseEntries)
}

func TestInMemoryPrepopulatedDirectoryFUSEReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory"): {
			Directory: childDirectory,
		},
		path.MustNewComponent("file"): {
			Leaf: childFile,
		},
	}, false))

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
		{Name: "directory", Mode: go_fuse.S_IFDIR, Ino: 101},
		{Name: "file", Mode: syscall.S_IFREG, Ino: 123},
	}, fuseEntries)
}

func TestInMemoryPrepopulatedDirectoryFUSEReadDirPlus(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("directory"): {
			Directory: childDirectory,
		},
		path.MustNewComponent("file"): {
			Leaf: childFile,
		},
	}, false))

	childFile.EXPECT().FUSEGetDirEntry().Return(go_fuse.DirEntry{
		Mode: go_fuse.S_IFREG,
		Ino:  123,
	}).AnyTimes()

	t.Run("Failure", func(t *testing.T) {
		// Calling FUSEReadDir() on a directory that fails to
		// initialize must return EIO.
		childDirectory.EXPECT().FetchContents().
			Return(nil, status.Error(codes.Unavailable, "Storage on fire"))
		errorLogger.EXPECT().Log(
			status.Error(
				codes.Unavailable,
				"Failed to initialize directory: Storage on fire"))

		d2, _, err := d.LookupChild(path.MustNewComponent("directory"))
		require.NoError(t, err)
		_, _, s := d2.FUSEReadDirPlus()
		require.Equal(t, go_fuse.EIO, s)
	})

	t.Run("Success", func(t *testing.T) {
		// Test the case where the directory is initialzed.
		directories, leaves, s := d.FUSEReadDirPlus()
		require.Equal(t, go_fuse.OK, s)
		require.Len(t, directories, 1)
		require.Equal(t, go_fuse.DirEntry{
			Name: "directory",
			Mode: go_fuse.S_IFDIR,
			Ino:  101,
		}, directories[0].DirEntry)
		require.ElementsMatch(t, []fuse.LeafDirEntry{
			{
				Child: childFile,
				DirEntry: go_fuse.DirEntry{
					Name: "file",
					Mode: go_fuse.S_IFREG,
					Ino:  123,
				},
			},
		}, leaves)

		// The directory handles returned by FUSEReadDirPlus()
		// should be actually usable.
		var attr go_fuse.Attr
		directories[0].Child.FUSEGetAttr(&attr)
		require.Equal(t, attr, go_fuse.Attr{
			Mode:  go_fuse.S_IFDIR | 0o777,
			Ino:   101,
			Nlink: fuse.ImplicitDirectoryLinkCount,
		})
	})
}

func TestInMemoryPrepopulatedDirectoryFUSERenameSelfDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Renaming a directory to itself should be permitted, even when
	// it is not empty.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("dir"))
	require.NoError(t, err)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	require.NoError(t, child.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("subdir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	require.Equal(t, go_fuse.OK, d.FUSERename(path.MustNewComponent("dir"), d, path.MustNewComponent("dir")))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory),
		})
}

func TestInMemoryPrepopulatedDirectoryFUSERenameSelfFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("a"): {
			Leaf: leaf,
		},
	}, false))

	leaf.EXPECT().FUSEGetAttr(gomock.Any()).Do(func(out *go_fuse.Attr) {
		out.Mode = go_fuse.S_IFREG | 0o666
		out.Ino = 3
		out.Size = 123
		out.Nlink = 1
	})
	var out go_fuse.Attr
	leaf.EXPECT().Link()
	require.Equal(t, go_fuse.OK, d.FUSELink(path.MustNewComponent("b"), leaf, &out))

	// Renaming a file to itself should have no effect. This even
	// applies to hard links. Though not intuitive, this means that
	// the source file may continue to exist.
	require.Equal(t, go_fuse.OK, d.FUSERename(path.MustNewComponent("a"), d, path.MustNewComponent("b")))

	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("a"), filesystem.FileTypeRegularFile),
			filesystem.NewFileInfo(path.MustNewComponent("b"), filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryPrepopulatedDirectoryFUSERenameDirectoryInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a reference to a removed child directory.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("removed"))
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("removed"))
	require.NoError(t, d.Remove(path.MustNewComponent("removed")))

	// Moving a directory into it should fail with ENOENT.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dir"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	require.Equal(t, go_fuse.ENOENT, d.FUSERename(path.MustNewComponent("dir"), child, path.MustNewComponent("dir")))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory),
		})
}

func TestInMemoryPrepopulatedDirectoryFUSERenameFileInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create a reference to a removed child directory.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("removed"))
	require.NoError(t, err)
	entryNotifier.EXPECT().Call(uint64(100), path.MustNewComponent("removed"))
	require.NoError(t, d.Remove(path.MustNewComponent("removed")))

	// Moving a file into it should fail with ENOENT.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("file"): {
			Leaf: leaf,
		},
	}, false))
	require.Equal(t, go_fuse.ENOENT, d.FUSERename(path.MustNewComponent("file"), child, path.MustNewComponent("file")))

	leaf.EXPECT().GetFileType().Return(filesystem.FileTypeRegularFile)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile),
		})
}

func TestInMemoryPrepopulatedDirectoryFUSERenameDirectoryTwice(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	// Create two empty directories.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(101))
	childA, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("a"))
	require.NoError(t, err)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(102))
	childB, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("b"))
	require.NoError(t, err)

	// Move "a" to "b" to "c". Afterwards, only "c" should remain.
	require.Equal(t, go_fuse.OK, d.FUSERename(path.MustNewComponent("a"), d, path.MustNewComponent("b")))
	require.Equal(t, go_fuse.OK, d.FUSERename(path.MustNewComponent("b"), d, path.MustNewComponent("c")))

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("c"), filesystem.FileTypeDirectory),
		})

	// Directory "a" got moved over "b", meaning that only the
	// former should still be usable. The latter has been deleted.
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(103))
	require.NoError(t, childA.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("subdirectory"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	require.Equal(t, syscall.ENOENT, childB.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("subdirectory"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
}

func TestInMemoryPrepopulatedDirectoryFUSERenameCrossDevice1(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier := mock.NewMockEntryNotifier(ctrl)
	d1 := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator, errorLogger, 100, inodeNumberGenerator, entryNotifier.Call)

	d2 := mock.NewMockFUSEDirectory(ctrl)

	// Attempting to rename a file to a directory that is of a
	// completely different type is not possible. We can only rename
	// objects between instances of InMemoryPrepopulatedDirectory.
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename(path.MustNewComponent("src"), d2, path.MustNewComponent("dst")))
}

func TestInMemoryPrepopulatedDirectoryFUSERenameCrossDevice2(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator1 := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier1 := mock.NewMockEntryNotifier(ctrl)
	d1 := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator1, errorLogger1, 100, inodeNumberGenerator1, entryNotifier1.Call)

	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator2 := mock.NewMockThreadSafeGenerator(ctrl)
	entryNotifier2 := mock.NewMockEntryNotifier(ctrl)
	d2 := fuse.NewInMemoryPrepopulatedDirectory(fileAllocator2, errorLogger2, 101, inodeNumberGenerator2, entryNotifier2.Call)

	// It should not be possible to rename directories from one
	// hierarchy to another, as this completely messes up
	// InMemoryPrepopulatedDirectory's internal bookkeeping.
	inodeNumberGenerator1.EXPECT().Uint64().Return(uint64(102))
	require.NoError(t, d1.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("src"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	inodeNumberGenerator2.EXPECT().Uint64().Return(uint64(103))
	require.NoError(t, d2.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("dst"): {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false))
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename(path.MustNewComponent("src"), d2, path.MustNewComponent("dst")))
	require.Equal(t, go_fuse.EXDEV, d1.FUSERename(path.MustNewComponent("src"), d2, path.MustNewComponent("nonexistent")))

	// Renaming files leaf files between directory hierarchies is
	// completely safe. It's generally not useful to do this, but
	// even if we disallowed this explicitly, it would still be
	// possible to achieve this by hardlinking.
	require.NoError(t, d1.CreateChildren(map[path.Component]fuse.InitialNode{
		path.MustNewComponent("leaf"): {
			Leaf: fuse.NewSymlink("target"),
		},
	}, false))
	require.Equal(t, go_fuse.OK, d1.FUSERename(path.MustNewComponent("leaf"), d2, path.MustNewComponent("leaf")))
}

// TODO: Missing testing coverage for FUSEMkdir(), FUSEReadDir(),
// FUSERename(), FUSERmdir(), FUSESymlink() and FUSEUnlink().
