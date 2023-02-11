package virtual_test

import (
	"context"
	"os"
	"regexp"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const inMemoryPrepopulatedDirectoryAttributesMask = virtual.AttributesMaskChangeID |
	virtual.AttributesMaskFileType |
	virtual.AttributesMaskInodeNumber |
	virtual.AttributesMaskLastDataModificationTime |
	virtual.AttributesMaskLinkCount |
	virtual.AttributesMaskPermissions |
	virtual.AttributesMaskSizeBytes

const specialFileAttributesMask = virtual.AttributesMaskChangeID |
	virtual.AttributesMaskFileType |
	virtual.AttributesMaskInodeNumber |
	virtual.AttributesMaskLinkCount |
	virtual.AttributesMaskPermissions |
	virtual.AttributesMaskSizeBytes

func inMemoryPrepopulatedDirectoryExpectMkdir(ctrl *gomock.Controller, handleAllocator *mock.MockStatefulHandleAllocator) *mock.MockStatefulDirectoryHandle {
	handleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(handleAllocation)
	directoryHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	handleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(directoryHandle)
	return directoryHandle
}

var hiddenFilesPatternForTesting = regexp.MustCompile("^\\._")

func TestInMemoryPrepopulatedDirectoryLookupChildNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	_, err := d.LookupChild(path.MustNewComponent("nonexistent"))
	require.True(t, os.IsNotExist(err))
}

func TestInMemoryPrepopulatedDirectoryLookupChildFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))

	child, err := d.LookupChild(path.MustNewComponent("file"))
	require.NoError(t, err)
	require.Equal(t, virtual.PrepopulatedDirectoryChild{}.FromLeaf(leaf), child)
}

func TestInMemoryPrepopulatedDirectoryLookupChildDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("subdir"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))

	child, err := d.LookupChild(path.MustNewComponent("subdir"))
	require.NoError(t, err)

	childDirectory, childLeaf := child.GetPair()
	require.NotNil(t, childDirectory)
	require.Nil(t, childLeaf)
}

func TestInMemoryPrepopulatedDirectoryLookupAllChildrenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("subdir"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
	}, false))

	child, err := d.LookupChild(path.MustNewComponent("subdir"))
	require.NoError(t, err)

	childDirectory, childLeaf := child.GetPair()
	require.NotNil(t, childDirectory)
	require.Nil(t, childLeaf)

	// When LookupAllChildren() is called in an uninitialized
	// directory and initialization fails, the error should be
	// propagated to the caller.
	initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
		Return(nil, status.Error(codes.Internal, "Network error"))
	_, _, err = childDirectory.LookupAllChildren()
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Network error"), err)
}

func TestInMemoryPrepopulatedDirectoryLookupAllChildrenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Populate the directory with files and directories.
	leaf1 := mock.NewMockNativeLeaf(ctrl)
	leaf2 := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("leaf1"):   virtual.InitialNode{}.FromLeaf(leaf1),
		path.MustNewComponent("._leaf2"): virtual.InitialNode{}.FromLeaf(leaf2),
	}, false))

	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	subdir1, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("subdir1"))
	require.NoError(t, err)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	subdir2, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("subdir2"))
	require.NoError(t, err)

	// All children should be returned in sorted order. Hidden
	// entries should be omitted.
	directories, leaves, err := d.LookupAllChildren()
	require.NoError(t, err)
	require.Equal(t, []virtual.DirectoryPrepopulatedDirEntry{
		{Name: path.MustNewComponent("subdir1"), Child: subdir1},
		{Name: path.MustNewComponent("subdir2"), Child: subdir2},
	}, directories)
	require.Equal(t, []virtual.LeafPrepopulatedDirEntry{
		{Name: path.MustNewComponent("leaf1"), Child: leaf1},
	}, leaves)
}

func TestInMemoryPrepopulatedDirectoryReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Prepare file system.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	leaf1 := mock.NewMockNativeLeaf(ctrl)
	leaf2 := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("directory"):     virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
		path.MustNewComponent("file"):          virtual.InitialNode{}.FromLeaf(leaf1),
		path.MustNewComponent("._hidden_file"): virtual.InitialNode{}.FromLeaf(leaf2),
	}, false))

	// Validate directory listing.
	leaf1.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(virtual.PermissionsRead)
	})
	files, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, files,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("directory"), filesystem.FileTypeDirectory, false),
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile, false),
		})
}

func TestInMemoryPrepopulatedDirectoryRemoveNonExistent(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	require.True(t, os.IsNotExist(d.Remove(path.MustNewComponent("nonexistent"))))
}

func TestInMemoryPrepopulatedDirectoryRemoveDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	subdirHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("directory"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))

	// Test that removing a directory through filesystem.Directory
	// also triggers FUSE invalidations.
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("directory"))
	subdirHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))
}

func TestInMemoryPrepopulatedDirectoryRemoveDirectoryNotEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("directory"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
	}, false))
	leaf := mock.NewMockNativeLeaf(ctrl)
	initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).Return(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(leaf),
	}, nil)

	require.Equal(t, syscall.ENOTEMPTY, d.Remove(path.MustNewComponent("directory")))
}

func TestInMemoryPrepopulatedDirectoryRemoveFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))

	leaf.EXPECT().Unlink()
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("file"))
	require.NoError(t, d.Remove(path.MustNewComponent("file")))
}

// TODO: Add testing coverage for RemoveAll().

func TestInMemoryPrepopulatedDirectoryCreateChildrenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Initial parent directory.
	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Merge another directory and file into it.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	subdirectoryFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	topLevelFile := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dir"):  virtual.InitialNode{}.FromDirectory(subdirectoryFetcher),
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(topLevelFile),
	}, false))

	// Validate top-level directory listing.
	topLevelFile.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
	})
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory, false),
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile, false),
		})

	// Validate subdirectory listing.
	child, err := d.LookupChild(path.MustNewComponent("dir"))
	require.NoError(t, err)
	subdirectoryFile := mock.NewMockNativeLeaf(ctrl)
	subdirectoryFetcher.EXPECT().FetchContents(gomock.Any()).Return(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(subdirectoryFile),
	}, nil)
	subdirectoryFile.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
	})
	subdirectory, _ := child.GetPair()
	entries, err = subdirectory.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile, false),
		})
}

func TestInMemoryPrepopulatedDirectoryCreateChildrenInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a reference to a removed child directory.
	childHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("directory"))
	childHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Merging files into the removed directory should fail.
	require.Equal(t, syscall.ENOENT, child.CreateChildren(map[path.Component]virtual.InitialNode{}, false))
}

func TestInMemoryPrepopulatedDirectoryInstallHooks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Initial top-level directory with custom hooks installed.
	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	symlinkFactory1 := mock.NewMockSymlinkFactory(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator1, symlinkFactory1, errorLogger1, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)
	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	d.InstallHooks(fileAllocator2, errorLogger2)

	// Validate that the top-level directory uses both the new file
	// allocator and error logger.
	fileAllocator2.EXPECT().NewFile(false, uint64(0)).Return(nil, virtual.StatusErrIO)
	var attr virtual.Attributes
	_, _, _, s := d.VirtualOpenChild(
		ctx,
		path.MustNewComponent("foo"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrIO, s)

	// Validate that a subdirectory uses the new file allocator
	// and error logger as well.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("dir"))
	require.NoError(t, err)
	fileAllocator2.EXPECT().NewFile(false, uint64(0)).Return(nil, virtual.StatusErrIO)
	_, _, _, s = child.VirtualOpenChild(
		ctx,
		path.MustNewComponent("foo"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrIO, s)
}

func TestInMemoryPrepopulatedDirectoryFilterChildren(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// In the initial state, InMemoryPrepopulatedDirectory will have
	// an EmptyInitialContentsFetcher associated with it.
	childFilter1 := mock.NewMockChildFilter(ctrl)
	childFilter1.EXPECT().Call(virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher), gomock.Any()).Return(true)
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
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	directory1 := mock.NewMockInitialContentsFetcher(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	directory2 := mock.NewMockInitialContentsFetcher(ctrl)
	leaf1 := mock.NewMockNativeLeaf(ctrl)
	leaf2 := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("directory1"): virtual.InitialNode{}.FromDirectory(directory1),
		path.MustNewComponent("directory2"): virtual.InitialNode{}.FromDirectory(directory2),
		path.MustNewComponent("leaf1"):      virtual.InitialNode{}.FromLeaf(leaf1),
		path.MustNewComponent("leaf2"):      virtual.InitialNode{}.FromLeaf(leaf2),
	}, false))

	childFilter3 := mock.NewMockChildFilter(ctrl)
	childFilter3.EXPECT().Call(virtual.InitialNode{}.FromDirectory(directory1), gomock.Any()).
		DoAndReturn(func(initialNode virtual.InitialNode, remove func() error) bool {
			require.NoError(t, remove())
			return true
		})
	childFilter3.EXPECT().Call(virtual.InitialNode{}.FromDirectory(directory2), gomock.Any()).Return(true)
	childFilter3.EXPECT().Call(virtual.InitialNode{}.FromLeaf(leaf1), gomock.Any()).
		DoAndReturn(func(initialNode virtual.InitialNode, remove func() error) bool {
			leaf1.EXPECT().Unlink()
			dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("leaf1"))
			require.NoError(t, remove())
			return true
		})
	childFilter3.EXPECT().Call(virtual.InitialNode{}.FromLeaf(leaf2), gomock.Any()).Return(true)
	require.NoError(t, d.FilterChildren(childFilter3.Call))

	// Another call to FilterChildren() should only report the
	// children that were not removed previously.
	childFilter4 := mock.NewMockChildFilter(ctrl)
	childFilter4.EXPECT().Call(virtual.InitialNode{}.FromDirectory(directory2), gomock.Any()).Return(true)
	childFilter4.EXPECT().Call(virtual.InitialNode{}.FromLeaf(leaf2), gomock.Any()).Return(true)
	require.NoError(t, d.FilterChildren(childFilter4.Call))
}

func TestInMemoryPrepopulatedDirectoryVirtualOpenChildFileExists(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a file at the desired target location.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("target"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))

	// Trying to create the file through FUSE should fail.
	var attr virtual.Attributes
	_, _, _, s := d.VirtualOpenChild(
		ctx,
		path.MustNewComponent("target"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrExist, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualOpenChildDirectoryExists(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a directory at the desired target location.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("target"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))

	// Trying to create the file through FUSE should fail.
	var attr virtual.Attributes
	_, _, _, s := d.VirtualOpenChild(
		ctx,
		path.MustNewComponent("target"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrExist, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualOpenChildAllocationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	fileAllocator.EXPECT().NewFile(false, uint64(0)).Return(nil, virtual.StatusErrIO)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// File allocation errors should translate to EIO. The actual
	// error should get forwarded to the error logger.
	var attr virtual.Attributes
	_, _, _, s := d.VirtualOpenChild(
		ctx,
		path.MustNewComponent("target"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrIO, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualOpenChildInRemovedDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a reference to a removed child directory.
	childHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("directory"))
	childHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Trying to create the file through FUSE should return ENOENT.
	var attr virtual.Attributes
	_, _, _, s := child.VirtualOpenChild(
		ctx,
		path.MustNewComponent("target"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMask(0),
		&attr)
	require.Equal(t, virtual.StatusErrNoEnt, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualOpenChildSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	fileAllocator.EXPECT().NewFile(false, uint64(0)).Return(child, virtual.StatusOK)
	child.EXPECT().VirtualGetAttributes(
		ctx,
		virtual.AttributesMaskInodeNumber,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetInodeNumber(123)
	})
	child.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(virtual.PermissionsRead)
	})
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Creation of the directory should fully succeed. The file
	// should be present within the directory afterwards.
	var attr virtual.Attributes
	newChild, respected, changeInfo, s := d.VirtualOpenChild(
		ctx,
		path.MustNewComponent("target"),
		virtual.ShareMaskWrite,
		(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
		nil,
		virtual.AttributesMaskInodeNumber,
		&attr)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, child, newChild)
	require.Equal(t, virtual.AttributesMaskPermissions, respected)
	require.Equal(t, virtual.ChangeInfo{
		Before: 0,
		After:  1,
	}, changeInfo)
	require.Equal(t, *(&virtual.Attributes{}).SetInodeNumber(123), attr)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("target"), filesystem.FileTypeRegularFile, false),
		})
}

func TestInMemoryPrepopulatedDirectoryVirtualGetAttributes(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock)

	dHandle.EXPECT().GetAttributes(inMemoryPrepopulatedDirectoryAttributesMask, gomock.Any()).
		Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetInodeNumber(100)
		})
	var attr1 virtual.Attributes
	d.VirtualGetAttributes(ctx, inMemoryPrepopulatedDirectoryAttributesMask, &attr1)
	require.Equal(
		t,
		*(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeDirectory).
			SetInodeNumber(100).
			SetLastDataModificationTime(time.Unix(1000, 0)).
			SetLinkCount(virtual.ImplicitDirectoryLinkCount).
			SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
			SetSizeBytes(0),
		attr1)
}

func TestInMemoryPrepopulatedDirectoryVirtualLinkExists(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Attempting to link to a file that already exists should fail.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dir"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	var attr virtual.Attributes
	_, s := d.VirtualLink(ctx, path.MustNewComponent("dir"), child, virtual.AttributesMask(0), &attr)
	require.Equal(t, virtual.StatusErrExist, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualLinkInRemovedDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	target := mock.NewMockNativeLeaf(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a reference to a removed child directory.
	childHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory"))
	require.NoError(t, err)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("directory"))
	childHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))

	// Linking a file into it should fail with ENOENT.
	var attr virtual.Attributes
	_, s := child.VirtualLink(ctx, path.MustNewComponent("target"), target, virtual.AttributesMask(0), &attr)
	require.Equal(t, virtual.StatusErrNoEnt, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualLinkNotNativeLeaf(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Trying to link a file that does not implement NativeLeaf is
	// not possible. We can only store leaf nodes that implement
	// this interface.
	child := mock.NewMockVirtualLeaf(ctrl)
	var attr virtual.Attributes
	_, s := d.VirtualLink(ctx, path.MustNewComponent("target"), child, virtual.AttributesMask(0), &attr)
	require.Equal(t, virtual.StatusErrXDev, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualLinkStale(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Attempting to link a file that has already been removed
	// should fail.
	child := mock.NewMockNativeLeaf(ctrl)
	child.EXPECT().Link().Return(virtual.StatusErrStale)

	var attr virtual.Attributes
	_, s := d.VirtualLink(ctx, path.MustNewComponent("target"), child, virtual.AttributesMaskInodeNumber, &attr)
	require.Equal(t, virtual.StatusErrStale, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualLinkSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	child := mock.NewMockNativeLeaf(ctrl)
	child.EXPECT().Link()
	child.EXPECT().VirtualGetAttributes(
		ctx,
		virtual.AttributesMaskInodeNumber,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetInodeNumber(123)
	})
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// We should return the attributes of the existing leaf.
	var attr virtual.Attributes
	changeInfo, s := d.VirtualLink(ctx, path.MustNewComponent("target"), child, virtual.AttributesMaskInodeNumber, &attr)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 0,
		After:  1,
	}, changeInfo)
	require.Equal(t, *(&virtual.Attributes{}).SetInodeNumber(123), attr)
}

func TestInMemoryPrepopulatedDirectoryVirtualLookup(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock)

	// Create an example directory and file that we'll try to look up.
	subdirHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	file := mock.NewMockNativeLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(3)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dir"):  virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(file),
	}, false))

	t.Run("NotFound", func(*testing.T) {
		var attr virtual.Attributes
		_, s := d.VirtualLookup(ctx, path.MustNewComponent("missing"), virtual.AttributesMask(0), &attr)
		require.Equal(t, virtual.StatusErrNoEnt, s)
	})

	t.Run("FoundDirectory", func(*testing.T) {
		subdirHandle.EXPECT().GetAttributes(inMemoryPrepopulatedDirectoryAttributesMask, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetInodeNumber(101)
			})

		var attr virtual.Attributes
		newChild, s := d.VirtualLookup(ctx, path.MustNewComponent("dir"), inMemoryPrepopulatedDirectoryAttributesMask, &attr)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(
			t,
			*(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetInodeNumber(101).
				SetLastDataModificationTime(time.Unix(1001, 0)).
				SetLinkCount(virtual.ImplicitDirectoryLinkCount).
				SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
				SetSizeBytes(0),
			attr)

		newDirectory, newLeaf := newChild.GetPair()
		require.NotNil(t, newDirectory)
		require.Nil(t, newLeaf)
	})

	t.Run("FoundFile", func(*testing.T) {
		file.EXPECT().VirtualGetAttributes(
			ctx,
			virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetInodeNumber(3)
		})

		var attr virtual.Attributes
		newChild, s := d.VirtualLookup(ctx, path.MustNewComponent("file"), virtual.AttributesMaskInodeNumber, &attr)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, virtual.DirectoryChild{}.FromLeaf(file), newChild)
		require.Equal(t, *(&virtual.Attributes{}).SetInodeNumber(3), attr)
	})
}

func TestInMemoryPrepopulatedDirectoryVirtualMkdir(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock)

	t.Run("FailureInitialContentsFetcher", func(t *testing.T) {
		// Create a subdirectory that has an initial contents fetcher.
		inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("subdir"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))

		child, err := d.LookupChild(path.MustNewComponent("subdir"))
		require.NoError(t, err)

		childDirectory, childLeaf := child.GetPair()
		require.NotNil(t, childDirectory)
		require.Nil(t, childLeaf)

		// Creating a directory in a directory whose initial
		// contents cannot be fetched, should fail. The reason
		// being that we can't accurately determine whether a
		// file under that name is already present.
		initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
			Return(nil, status.Error(codes.Internal, "Network error"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to initialize directory: Network error")))

		_, _, s := childDirectory.VirtualMkdir(path.MustNewComponent("subsubdir"), 0, &virtual.Attributes{})
		require.Equal(t, virtual.StatusErrIO, s)
	})

	t.Run("FailureExist", func(t *testing.T) {
		// The operation should fail if a file or directory
		// already exists under the provided name.
		existingFile := mock.NewMockNativeLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("existing_file"): virtual.InitialNode{}.FromLeaf(existingFile),
		}, false))

		_, _, s := d.VirtualMkdir(path.MustNewComponent("existing_file"), 0, &virtual.Attributes{})
		require.Equal(t, virtual.StatusErrExist, s)
	})

	t.Run("Success", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(2)
		subdirHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		subdirHandle.EXPECT().GetAttributes(inMemoryPrepopulatedDirectoryAttributesMask, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetInodeNumber(101)
			})

		var out virtual.Attributes
		leaf, changeInfo, s := d.VirtualMkdir(path.MustNewComponent("dir"), inMemoryPrepopulatedDirectoryAttributesMask, &out)
		require.Equal(t, virtual.StatusOK, s)
		require.NotNil(t, leaf)
		require.Equal(t, virtual.ChangeInfo{
			Before: 2,
			After:  3,
		}, changeInfo)
		require.Equal(
			t,
			*(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetInodeNumber(101).
				SetLastDataModificationTime(time.Unix(1003, 0)).
				SetLinkCount(virtual.ImplicitDirectoryLinkCount).
				SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
				SetSizeBytes(0),
			out)
	})
}

func TestInMemoryPrepopulatedDirectoryVirtualMknodExists(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Files may not be overwritten by mknod().
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dir"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	var attr virtual.Attributes
	_, _, s := d.VirtualMknod(ctx, path.MustNewComponent("dir"), filesystem.FileTypeFIFO, virtual.AttributesMask(0), &attr)
	require.Equal(t, virtual.StatusErrExist, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualMknodSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a FIFO and a UNIX domain socket.
	fifoHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(fifoHandleAllocation)
	fifoHandleAllocation.EXPECT().AsNativeLeaf(gomock.Any()).
		DoAndReturn(func(leaf virtual.NativeLeaf) virtual.NativeLeaf { return leaf })
	var fifoAttr virtual.Attributes
	fifoNode, changeInfo, s := d.VirtualMknod(ctx, path.MustNewComponent("fifo"), filesystem.FileTypeFIFO, specialFileAttributesMask, &fifoAttr)
	require.Equal(t, virtual.StatusOK, s)
	require.NotNil(t, fifoNode)
	require.Equal(t, virtual.ChangeInfo{
		Before: 0,
		After:  1,
	}, changeInfo)
	require.Equal(
		t,
		*(&virtual.Attributes{}).
			SetChangeID(0).
			SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite).
			SetFileType(filesystem.FileTypeFIFO).
			SetSizeBytes(0),
		fifoAttr)

	socketHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(socketHandleAllocation)
	socketHandleAllocation.EXPECT().AsNativeLeaf(gomock.Any()).
		DoAndReturn(func(leaf virtual.NativeLeaf) virtual.NativeLeaf { return leaf })
	var socketAttr virtual.Attributes
	socketNode, changeInfo, s := d.VirtualMknod(ctx, path.MustNewComponent("socket"), filesystem.FileTypeSocket, specialFileAttributesMask, &socketAttr)
	require.Equal(t, virtual.StatusOK, s)
	require.NotNil(t, socketNode)
	require.Equal(t, virtual.ChangeInfo{
		Before: 1,
		After:  2,
	}, changeInfo)
	require.Equal(
		t,
		*(&virtual.Attributes{}).
			SetChangeID(0).
			SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite).
			SetFileType(filesystem.FileTypeSocket).
			SetSizeBytes(0),
		socketAttr)

	// Check whether the devices are reported properly using the
	// native ReadDir() method.
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("fifo"), filesystem.FileTypeFIFO, false),
			filesystem.NewFileInfo(path.MustNewComponent("socket"), filesystem.FileTypeSocket, false),
		}, entries)

	// Check whether the devices are reported properly using the
	// VirtualReadDir() method.
	reporter := mock.NewMockDirectoryEntryReporter(ctrl)
	reporter.EXPECT().ReportEntry(uint64(1), path.MustNewComponent("fifo"), virtual.DirectoryChild{}.FromLeaf(fifoNode), &fifoAttr).Return(true)
	reporter.EXPECT().ReportEntry(uint64(2), path.MustNewComponent("socket"), virtual.DirectoryChild{}.FromLeaf(socketNode), &socketAttr).Return(true)
	require.Equal(t, virtual.StatusOK, d.VirtualReadDir(ctx, 0, specialFileAttributesMask, reporter))
}

func TestInMemoryPrepopulatedDirectoryVirtualReadDir(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock)

	// Populate the directory with subdirectory that is
	// uninitialized and a file.
	childDirectoryHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	childDirectory := mock.NewMockInitialContentsFetcher(ctrl)
	childFile1 := mock.NewMockNativeLeaf(ctrl)
	childFile2 := mock.NewMockNativeLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(4)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("directory"):     virtual.InitialNode{}.FromDirectory(childDirectory),
		path.MustNewComponent("file"):          virtual.InitialNode{}.FromLeaf(childFile1),
		path.MustNewComponent("._hidden_file"): virtual.InitialNode{}.FromLeaf(childFile2),
	}, false))

	// Obtaining the directory listing through VirtualReadDir() should
	// not cause the child directory to be initialized. We don't
	// depend on any of its properties to populate its DirEntry, nor
	// are we returning a handle to it. A successive VirtualLookup()
	// call will initialize the directory.
	childDirectoryHandle.EXPECT().GetAttributes(inMemoryPrepopulatedDirectoryAttributesMask, gomock.Any()).
		Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetInodeNumber(101)
		})
	childFile1.EXPECT().VirtualGetAttributes(
		ctx,
		inMemoryPrepopulatedDirectoryAttributesMask,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetInodeNumber(123)
	})
	reporter := mock.NewMockDirectoryEntryReporter(ctrl)
	reporter.EXPECT().ReportEntry(
		uint64(2),
		path.MustNewComponent("directory"),
		gomock.Any(),
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeDirectory).
			SetInodeNumber(101).
			SetLastDataModificationTime(time.Unix(1001, 0)).
			SetLinkCount(virtual.ImplicitDirectoryLinkCount).
			SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite|virtual.PermissionsExecute).
			SetSizeBytes(0),
	).Return(true)
	reporter.EXPECT().ReportEntry(
		uint64(3),
		path.MustNewComponent("file"),
		virtual.DirectoryChild{}.FromLeaf(childFile1),
		(&virtual.Attributes{}).
			SetFileType(filesystem.FileTypeRegularFile).
			SetInodeNumber(123),
	).Return(true)

	require.Equal(t, virtual.StatusOK, d.VirtualReadDir(ctx, 0, inMemoryPrepopulatedDirectoryAttributesMask, reporter))
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameSelfDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Renaming a directory to itself should be permitted, even when
	// it is not empty.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("dir"))
	require.NoError(t, err)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, child.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("subdir"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	changeInfo1, changeInfo2, s := d.VirtualRename(path.MustNewComponent("dir"), d, path.MustNewComponent("dir"))
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 1,
		After:  1,
	}, changeInfo1)
	require.Equal(t, virtual.ChangeInfo{
		Before: 1,
		After:  1,
	}, changeInfo2)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory, false),
		})
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameSelfFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("a"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))

	leaf.EXPECT().VirtualGetAttributes(
		ctx,
		virtual.AttributesMaskInodeNumber,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetInodeNumber(3)
	})
	var out virtual.Attributes
	leaf.EXPECT().Link()
	changeInfo, s := d.VirtualLink(ctx, path.MustNewComponent("b"), leaf, virtual.AttributesMaskInodeNumber, &out)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 1,
		After:  2,
	}, changeInfo)
	require.Equal(t, *(&virtual.Attributes{}).SetInodeNumber(3), out)

	// Renaming a file to itself should have no effect. This even
	// applies to hard links. Though not intuitive, this means that
	// the source file may continue to exist.
	changeInfo1, changeInfo2, s := d.VirtualRename(path.MustNewComponent("a"), d, path.MustNewComponent("b"))
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 2,
		After:  2,
	}, changeInfo1)
	require.Equal(t, virtual.ChangeInfo{
		Before: 2,
		After:  2,
	}, changeInfo2)

	leaf.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(0)
	}).Times(2)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("a"), filesystem.FileTypeRegularFile, false),
			filesystem.NewFileInfo(path.MustNewComponent("b"), filesystem.FileTypeRegularFile, false),
		})
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameDirectoryInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a reference to a removed child directory.
	childHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("removed"))
	require.NoError(t, err)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("removed"))
	childHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("removed")))

	// Moving a directory into it should fail with ENOENT.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dir"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	_, _, s := d.VirtualRename(path.MustNewComponent("dir"), child, path.MustNewComponent("dir"))
	require.Equal(t, virtual.StatusErrNoEnt, s)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("dir"), filesystem.FileTypeDirectory, false),
		})
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameFileInRemovedDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create a reference to a removed child directory.
	childHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	child, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("removed"))
	require.NoError(t, err)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("removed"))
	childHandle.EXPECT().Release()
	require.NoError(t, d.Remove(path.MustNewComponent("removed")))

	// Moving a file into it should fail with ENOENT.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))
	_, _, s := d.VirtualRename(path.MustNewComponent("file"), child, path.MustNewComponent("file"))
	require.Equal(t, virtual.StatusErrNoEnt, s)

	leaf.EXPECT().VirtualGetAttributes(
		gomock.Any(),
		virtual.AttributesMaskFileType|virtual.AttributesMaskPermissions,
		gomock.Any(),
	).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
		attributes.SetFileType(filesystem.FileTypeRegularFile)
		attributes.SetPermissions(0)
	})
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile, false),
		})
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameDirectoryTwice(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// Create two empty directories.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	childA, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("a"))
	require.NoError(t, err)
	childBHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	childB, err := d.CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("b"))
	require.NoError(t, err)

	// Move "a" to "b" to "c". Afterwards, only "c" should remain.
	childBHandle.EXPECT().Release()
	changeInfo1, changeInfo2, s := d.VirtualRename(path.MustNewComponent("a"), d, path.MustNewComponent("b"))
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 2,
		After:  5,
	}, changeInfo1)
	require.Equal(t, virtual.ChangeInfo{
		Before: 2,
		After:  5,
	}, changeInfo2)
	changeInfo1, changeInfo2, s = d.VirtualRename(path.MustNewComponent("b"), d, path.MustNewComponent("c"))
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 5,
		After:  7,
	}, changeInfo1)
	require.Equal(t, virtual.ChangeInfo{
		Before: 5,
		After:  7,
	}, changeInfo2)

	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, entries,
		[]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("c"), filesystem.FileTypeDirectory, false),
		})

	// Directory "a" got moved over "b", meaning that only the
	// former should still be usable. The latter has been deleted.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	require.NoError(t, childA.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("subdirectory"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	require.Equal(t, syscall.ENOENT, childB.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("subdirectory"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameCrossDevice1(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d1 := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	d2 := mock.NewMockVirtualDirectory(ctrl)

	// Attempting to rename a file to a directory that is of a
	// completely different type is not possible. We can only rename
	// objects between instances of InMemoryPrepopulatedDirectory.
	_, _, s := d1.VirtualRename(path.MustNewComponent("src"), d2, path.MustNewComponent("dst"))
	require.Equal(t, virtual.StatusErrXDev, s)
}

func TestInMemoryPrepopulatedDirectoryVirtualRenameCrossDevice2(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator1 := mock.NewMockFileAllocator(ctrl)
	symlinkFactory1 := mock.NewMockSymlinkFactory(ctrl)
	errorLogger1 := mock.NewMockErrorLogger(ctrl)
	handleAllocator1 := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator1)
	d1 := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator1, symlinkFactory1, errorLogger1, handleAllocator1, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	fileAllocator2 := mock.NewMockFileAllocator(ctrl)
	symlinkFactory2 := mock.NewMockSymlinkFactory(ctrl)
	errorLogger2 := mock.NewMockErrorLogger(ctrl)
	handleAllocator2 := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator2)
	d2 := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator2, symlinkFactory2, errorLogger2, handleAllocator2, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	// It should not be possible to rename directories from one
	// hierarchy to another, as this completely messes up
	// InMemoryPrepopulatedDirectory's internal bookkeeping.
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator1)
	require.NoError(t, d1.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("src"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator2)
	require.NoError(t, d2.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("dst"): virtual.InitialNode{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false))
	_, _, s := d1.VirtualRename(path.MustNewComponent("src"), d2, path.MustNewComponent("dst"))
	require.Equal(t, virtual.StatusErrXDev, s)
	_, _, s = d1.VirtualRename(path.MustNewComponent("src"), d2, path.MustNewComponent("nonexistent"))
	require.Equal(t, virtual.StatusErrXDev, s)

	// Renaming files leaf files between directory hierarchies is
	// completely safe. It's generally not useful to do this, but
	// even if we disallowed this explicitly, it would still be
	// possible to achieve this by hardlinking.
	leaf := mock.NewMockNativeLeaf(ctrl)
	require.NoError(t, d1.CreateChildren(map[path.Component]virtual.InitialNode{
		path.MustNewComponent("leaf"): virtual.InitialNode{}.FromLeaf(leaf),
	}, false))
	changeInfo1, changeInfo2, s := d1.VirtualRename(path.MustNewComponent("leaf"), d2, path.MustNewComponent("leaf"))
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, virtual.ChangeInfo{
		Before: 2,
		After:  3,
	}, changeInfo1)
	require.Equal(t, virtual.ChangeInfo{
		Before: 1,
		After:  2,
	}, changeInfo2)
}

func TestInMemoryPrepopulatedDirectoryVirtualRemove(t *testing.T) {
	ctrl := gomock.NewController(t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	t.Run("NotFound", func(t *testing.T) {
		// Attempting to remove a file that does not exist.
		_, s := d.VirtualRemove(path.MustNewComponent("nonexistent"), true, true)
		require.Equal(t, virtual.StatusErrNoEnt, s)
	})

	t.Run("NoDirectoryRemoval", func(t *testing.T) {
		// Attempting to remove a directory, even though
		// directory removal should not be performed.
		inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("no_directory_removal"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))

		_, s := d.VirtualRemove(path.MustNewComponent("no_directory_removal"), false, true)
		require.Equal(t, virtual.StatusErrPerm, s)
	})

	t.Run("NoLeafRemoval", func(t *testing.T) {
		// Attempting to remove a leaf, even though leaf removal
		// should not be performed.
		leaf := mock.NewMockNativeLeaf(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("no_file_removal"): virtual.InitialNode{}.FromLeaf(leaf),
		}, false))

		_, s := d.VirtualRemove(path.MustNewComponent("no_file_removal"), true, false)
		require.Equal(t, virtual.StatusErrNotDir, s)
	})

	t.Run("ChildDirectoryInitializationFailure", func(t *testing.T) {
		// If we cannot load the directory contents, we don't
		// know whether the directory is empty and can be
		// removed.
		inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("broken_directory"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))
		initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
			Return(nil, status.Error(codes.Internal, "Network error"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to initialize directory: Network error")))

		_, s := d.VirtualRemove(path.MustNewComponent("broken_directory"), true, false)
		require.Equal(t, virtual.StatusErrIO, s)
	})

	t.Run("ChildDirectoryNotEmpty", func(t *testing.T) {
		inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("non_empty_directory"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))
		leaf := mock.NewMockNativeLeaf(ctrl)
		initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).Return(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(leaf),
		}, nil)

		_, s := d.VirtualRemove(path.MustNewComponent("non_empty_directory"), true, false)
		require.Equal(t, virtual.StatusErrNotEmpty, s)
	})

	t.Run("SuccessFile", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("success"): virtual.InitialNode{}.FromLeaf(leaf),
		}, false))
		leaf.EXPECT().Unlink()

		changeInfo, s := d.VirtualRemove(path.MustNewComponent("success"), true, true)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, virtual.ChangeInfo{
			Before: 5,
			After:  6,
		}, changeInfo)
	})

	t.Run("SuccessDirectory", func(t *testing.T) {
		// Directories may be removed, even if they are not
		// empty. In that case they should exclusively consist
		// of hidden files.
		dHandle := inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("directory_with_hidden_files"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))
		leaf1 := mock.NewMockNativeLeaf(ctrl)
		leaf2 := mock.NewMockNativeLeaf(ctrl)
		initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).Return(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("._hidden_file1"): virtual.InitialNode{}.FromLeaf(leaf1),
			path.MustNewComponent("._hidden_file2"): virtual.InitialNode{}.FromLeaf(leaf2),
		}, nil)
		leaf1.EXPECT().Unlink()
		leaf2.EXPECT().Unlink()
		dHandle.EXPECT().Release()

		changeInfo, s := d.VirtualRemove(path.MustNewComponent("directory_with_hidden_files"), true, true)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, virtual.ChangeInfo{
			Before: 7,
			After:  8,
		}, changeInfo)
	})
}

func TestInMemoryPrepopulatedDirectoryVirtualSymlink(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileAllocator := mock.NewMockFileAllocator(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
	d := virtual.NewInMemoryPrepopulatedDirectory(fileAllocator, symlinkFactory, errorLogger, handleAllocator, sort.Sort, hiddenFilesPatternForTesting.MatchString, clock.SystemClock)

	t.Run("FailureInitialContentsFetcher", func(t *testing.T) {
		// Create a subdirectory that has an initial contents fetcher.
		inMemoryPrepopulatedDirectoryExpectMkdir(ctrl, handleAllocator)
		initialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("subdir"): virtual.InitialNode{}.FromDirectory(initialContentsFetcher),
		}, false))

		child, err := d.LookupChild(path.MustNewComponent("subdir"))
		require.NoError(t, err)

		childDirectory, childLeaf := child.GetPair()
		require.NotNil(t, childDirectory)
		require.Nil(t, childLeaf)

		// Creating a symlink in a directory whose initial
		// contents cannot be fetched, should fail. The reason
		// being that we can't accurately determine whether a
		// file under that name is already present.
		initialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
			Return(nil, status.Error(codes.Internal, "Network error"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to initialize directory: Network error")))

		_, _, s := childDirectory.VirtualSymlink(ctx, []byte("target"), path.MustNewComponent("symlink"), 0, &virtual.Attributes{})
		require.Equal(t, virtual.StatusErrIO, s)
	})

	t.Run("FailureExist", func(t *testing.T) {
		// The operation should fail if a file or directory
		// already exists under the provided name.
		existingFile := mock.NewMockNativeLeaf(ctrl)
		require.NoError(t, d.CreateChildren(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("existing_file"): virtual.InitialNode{}.FromLeaf(existingFile),
		}, false))

		_, _, s := d.VirtualSymlink(ctx, []byte("target"), path.MustNewComponent("existing_file"), 0, &virtual.Attributes{})
		require.Equal(t, virtual.StatusErrExist, s)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		symlinkFactory.EXPECT().LookupSymlink([]byte("target")).Return(leaf)
		leaf.EXPECT().VirtualGetAttributes(
			ctx,
			virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetInodeNumber(3)
		})

		var out virtual.Attributes
		actualLeaf, changeInfo, s := d.VirtualSymlink(ctx, []byte("target"), path.MustNewComponent("symlink"), virtual.AttributesMaskInodeNumber, &out)
		require.Equal(t, virtual.StatusOK, s)
		require.NotNil(t, actualLeaf)
		require.Equal(t, virtual.ChangeInfo{
			Before: 2,
			After:  3,
		}, changeInfo)
		require.Equal(t, (&virtual.Attributes{}).SetInodeNumber(3), &out)
	})
}
