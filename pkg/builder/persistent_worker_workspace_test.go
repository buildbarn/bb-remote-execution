package builder_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type persistentWorkspaceTestStorage struct {
	test        *testing.T
	function    digest.Function
	directories map[digest.Digest]*remoteexecution.Directory
	contents    map[digest.Digest][]byte
	fetcher     *mock.MockDirectoryFetcher
	blobs       *mock.MockBlobAccess
}

func newPersistentWorkspaceTestStorage(test *testing.T) *persistentWorkspaceTestStorage {
	ctrl := gomock.NewController(test)
	storage := &persistentWorkspaceTestStorage{
		test:        test,
		function:    digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256),
		directories: map[digest.Digest]*remoteexecution.Directory{},
		contents:    map[digest.Digest][]byte{},
		fetcher:     mock.NewMockDirectoryFetcher(ctrl),
		blobs:       mock.NewMockBlobAccess(ctrl),
	}
	storage.fetcher.EXPECT().GetDirectory(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if directory, ok := storage.directories[directoryDigest]; ok {
			return directory, nil
		}
		return nil, status.Error(codes.NotFound, "Missing directory")
	}).AnyTimes()
	storage.blobs.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
		if err := ctx.Err(); err != nil {
			return buffer.NewBufferFromError(err)
		}
		contents, ok := storage.contents[blobDigest]
		if !ok {
			return buffer.NewBufferFromError(status.Error(codes.NotFound, "Missing file"))
		}
		return buffer.NewValidatedBufferFromByteSlice(contents)
	}).AnyTimes()
	return storage
}

func (storage *persistentWorkspaceTestStorage) addBytes(contents []byte) digest.Digest {
	generator := storage.function.NewGenerator(int64(len(contents)))
	_, err := generator.Write(contents)
	require.NoError(storage.test, err)
	blobDigest := generator.Sum()
	storage.contents[blobDigest] = contents
	return blobDigest
}

func (storage *persistentWorkspaceTestStorage) addDirectory(directory *remoteexecution.Directory) digest.Digest {
	contents, err := proto.Marshal(directory)
	require.NoError(storage.test, err)
	directoryDigest := storage.addBytes(contents)
	storage.directories[directoryDigest] = directory
	return directoryDigest
}

func (storage *persistentWorkspaceTestStorage) newNativeRoot() (builder.BuildDirectory, string) {
	directoryPath := storage.test.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(storage.test, err)
	storage.test.Cleanup(func() { require.NoError(storage.test, directory.Close()) })
	return builder.NewNaiveBuildDirectory(directory, storage.fetcher, cas.NewBlobAccessFileFetcher(storage.blobs), semaphore.NewWeighted(1), storage.blobs), directoryPath
}

func persistentWorkspaceTestOutputs(test *testing.T, workingDirectory string) *builder.OutputHierarchy {
	outputs, err := builder.NewOutputHierarchy(&remoteexecution.Command{WorkingDirectory: workingDirectory, OutputPaths: []string{"out/result"}})
	require.NoError(test, err)
	return outputs
}

func TestPersistentWorkerWorkspaceReconciliation(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	var cleans int
	creator := builder.NewSharedBuildDirectoryCreator(builder.NewCleanBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), cleaner.NewIdleInvoker(func(context.Context) error {
		cleans++
		return nil
	})), &nextID)
	workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "pkg/work", nil, nil)
	require.NoError(test, err)
	workspacePath := filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString())
	workingPath := filepath.Join(workspacePath, "root", "pkg", "work")
	outputs := persistentWorkspaceTestOutputs(test, "pkg/work")
	firstContents := storage.addBytes([]byte("first"))
	firstWork := storage.addDirectory(&remoteexecution.Directory{Files: []*remoteexecution.FileNode{{Name: "source", Digest: firstContents.GetProto()}, {Name: "removed", Digest: firstContents.GetProto()}}})
	firstPackage := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "work", Digest: firstWork.GetProto()}}})
	firstRoot := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "pkg", Digest: firstPackage.GetProto()}}})
	firstContext, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstLease, err := workspace.Prepare(firstContext, firstRoot, outputs)
	require.NoError(test, err)
	workingHandle, err := os.Open(workingPath)
	require.NoError(test, err)
	defer workingHandle.Close()
	workingInfo, err := workingHandle.Stat()
	require.NoError(test, err)
	for _, filename := range []string{"root/pkg/work/out/result", "root/pkg/work/undeclared", "stdout", "stderr", "server_logs/old", "session_logs/stderr", "tmp/compiler-cache"} {
		require.NoError(test, os.WriteFile(filepath.Join(workspacePath, filename), []byte("old"), 0o600))
	}
	outsidePath := test.TempDir()
	require.NoError(test, os.WriteFile(filepath.Join(outsidePath, "keep"), []byte("safe"), 0o600))
	require.NoError(test, os.Symlink(outsidePath, filepath.Join(workingPath, "stale-link")))
	_, err = workspace.Prepare(context.Background(), firstRoot, outputs)
	require.Error(test, err)
	stopCalls := 0
	stopWorker := func(context.Context) error {
		stopCalls++
		_, err := os.Stat(workingPath)
		require.NoError(test, err)
		require.NoError(test, workingHandle.Close())
		return nil
	}
	require.Error(test, workspace.Close(context.Background(), stopWorker))
	require.Zero(test, stopCalls)
	firstLease.Release()
	firstLease.Release()
	cancelFirst()
	require.Equal(test, 1, cleans)
	_, err = os.Stat(filepath.Join(workingPath, "out/result"))
	require.NoError(test, err)

	secondContents := storage.addBytes([]byte("second"))
	secondWork := storage.addDirectory(&remoteexecution.Directory{
		Files:    []*remoteexecution.FileNode{{Name: "source", Digest: secondContents.GetProto(), IsExecutable: true}},
		Symlinks: []*remoteexecution.SymlinkNode{{Name: "new-link", Target: "source"}},
	})
	secondPackage := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "work", Digest: secondWork.GetProto()}}})
	secondRoot := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "pkg", Digest: secondPackage.GetProto()}}})
	secondLease, err := workspace.Prepare(context.Background(), secondRoot, outputs)
	require.NoError(test, err)
	newWorkingInfo, err := os.Stat(workingPath)
	require.NoError(test, err)
	require.True(test, os.SameFile(workingInfo, newWorkingInfo))
	contents, err := os.ReadFile(filepath.Join(workingPath, "source"))
	require.NoError(test, err)
	require.Equal(test, "second", string(contents))
	sourceInfo, err := os.Stat(filepath.Join(workingPath, "source"))
	require.NoError(test, err)
	if runtime.GOOS != "windows" {
		require.NotZero(test, sourceInfo.Mode()&0o111)
	}
	target, err := os.Readlink(filepath.Join(workingPath, "new-link"))
	require.NoError(test, err)
	require.Equal(test, "source", target)
	for _, filename := range []string{"root/pkg/work/out/result", "root/pkg/work/removed", "root/pkg/work/undeclared", "root/pkg/work/stale-link", "stdout", "stderr", "server_logs/old"} {
		_, err := os.Lstat(filepath.Join(workspacePath, filename))
		require.True(test, os.IsNotExist(err))
	}
	for _, filename := range []string{"session_logs/stderr", "tmp/compiler-cache"} {
		_, err := os.Stat(filepath.Join(workspacePath, filename))
		require.NoError(test, err)
	}
	_, err = os.Stat(filepath.Join(outsidePath, "keep"))
	require.NoError(test, err)
	secondLease.Release()
	require.NoError(test, workspace.Close(context.Background(), stopWorker))
	require.NoError(test, workspace.Close(context.Background(), stopWorker))
	require.Equal(test, 1, stopCalls)
	require.Equal(test, 2, cleans)
	_, err = os.Stat(workspacePath)
	require.True(test, os.IsNotExist(err))
}

func TestPersistentWorkerWorkspaceRetainsToolFiles(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	creator := builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID)
	workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "work", []string{"tools/bin/compiler"}, nil)
	require.NoError(test, err)
	defer func() {
		require.NoError(test, workspace.Close(context.Background(), func(context.Context) error { return nil }))
	}()
	toolDigest := storage.addBytes([]byte("compiler"))
	inputRoot := func(source string) digest.Digest {
		sourceDigest := storage.addBytes([]byte(source))
		binaryDirectory := storage.addDirectory(&remoteexecution.Directory{Files: []*remoteexecution.FileNode{
			{Name: "compiler", Digest: toolDigest.GetProto(), IsExecutable: true},
			{Name: "source", Digest: sourceDigest.GetProto()},
		}})
		toolDirectory := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "bin", Digest: binaryDirectory.GetProto()}}})
		return storage.addDirectory(&remoteexecution.Directory{
			Files:       []*remoteexecution.FileNode{{Name: "compiler", Digest: sourceDigest.GetProto()}},
			Directories: []*remoteexecution.DirectoryNode{{Name: "tools", Digest: toolDirectory.GetProto()}},
		})
	}
	outputs := persistentWorkspaceTestOutputs(test, "work")
	firstLease, err := workspace.Prepare(context.Background(), inputRoot("first"), outputs)
	require.NoError(test, err)
	firstLease.Release()
	rootPath := filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString(), "root")
	toolPath := filepath.Join(rootPath, "tools", "bin", "compiler")
	toolFile, err := os.Open(toolPath)
	require.NoError(test, err)
	defer toolFile.Close()
	toolInfo, err := toolFile.Stat()
	require.NoError(test, err)
	stalePath := filepath.Join(rootPath, "tools", "bin", "stale")
	require.NoError(test, os.WriteFile(stalePath, []byte("stale"), 0o666))
	delete(storage.contents, toolDigest)
	secondLease, err := workspace.Prepare(context.Background(), inputRoot("second"), outputs)
	require.NoError(test, err)
	defer secondLease.Release()
	newToolInfo, err := os.Stat(toolPath)
	require.NoError(test, err)
	require.True(test, os.SameFile(toolInfo, newToolInfo))
	for _, sourcePath := range []string{"compiler", "tools/bin/source"} {
		contents, err := os.ReadFile(filepath.Join(rootPath, sourcePath))
		require.NoError(test, err)
		require.Equal(test, "second", string(contents))
	}
	require.NoFileExists(test, stalePath)
}

func TestPersistentWorkerWorkspaceToolFileReplaced(test *testing.T) {
	for _, replacement := range []string{"Missing", "Directory", "Symlink"} {
		test.Run(replacement, func(test *testing.T) {
			storage := newPersistentWorkspaceTestStorage(test)
			rootDirectory, nativePath := storage.newNativeRoot()
			var nextID atomic.Uint64
			creator := builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID)
			workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "work", []string{"compiler"}, nil)
			require.NoError(test, err)
			defer func() {
				require.NoError(test, workspace.Close(context.Background(), func(context.Context) error { return nil }))
			}()
			inputDigest := storage.addDirectory(&remoteexecution.Directory{Files: []*remoteexecution.FileNode{{Name: "compiler", Digest: storage.addBytes([]byte("tool")).GetProto()}}})
			outputs := persistentWorkspaceTestOutputs(test, "work")
			lease, err := workspace.Prepare(context.Background(), inputDigest, outputs)
			require.NoError(test, err)
			lease.Release()
			toolPath := filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString(), "root", "compiler")
			require.NoError(test, os.Chmod(toolPath, 0o666))
			require.NoError(test, os.Remove(toolPath))
			switch replacement {
			case "Directory":
				require.NoError(test, os.Mkdir(toolPath, 0o777))
			case "Symlink":
				require.NoError(test, os.Symlink("work", toolPath))
			}
			_, err = workspace.Prepare(context.Background(), inputDigest, outputs)
			require.Error(test, err)
		})
	}
}

func TestPersistentWorkerWorkspaceRetirement(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	creator := builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID)
	first, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "", nil, nil)
	require.NoError(test, err)
	second, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "", nil, nil)
	require.NoError(test, err)
	require.NotEqual(test, first.GetBuildDirectoryPath().GetUNIXString(), second.GetBuildDirectoryPath().GetUNIXString())
	firstPath := filepath.Join(nativePath, first.GetBuildDirectoryPath().GetUNIXString())
	require.Error(test, first.Close(context.Background(), func(context.Context) error {
		return status.Error(codes.Unavailable, "Runner disconnected")
	}))
	_, err = os.Stat(firstPath)
	require.NoError(test, err)
	_, err = first.Prepare(context.Background(), storage.addDirectory(&remoteexecution.Directory{}), persistentWorkspaceTestOutputs(test, ""))
	require.Error(test, err)
	require.NoError(test, first.Close(context.Background(), func(context.Context) error { return nil }))
	_, err = os.Stat(firstPath)
	require.True(test, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(nativePath, second.GetBuildDirectoryPath().GetUNIXString()))
	require.NoError(test, err)
	require.NoError(test, second.Close(context.Background(), func(context.Context) error { return nil }))
}

func TestPersistentWorkerWorkspaceCloseExcludesPreparation(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID), pool.EmptyFilePool, "", nil, nil)
	require.NoError(test, err)
	workspacePath := filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString())
	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- workspace.Close(context.Background(), func(context.Context) error {
			close(stopEntered)
			<-allowStop
			return nil
		})
	}()
	<-stopEntered
	_, err = workspace.Prepare(context.Background(), storage.addDirectory(&remoteexecution.Directory{}), persistentWorkspaceTestOutputs(test, ""))
	require.Error(test, err)
	_, err = os.Stat(workspacePath)
	require.NoError(test, err)
	close(allowStop)
	require.NoError(test, <-closeDone)
	_, err = os.Stat(workspacePath)
	require.True(test, os.IsNotExist(err))
}

func TestPersistentWorkerWorkspaceInvalidWorkingDirectory(test *testing.T) {
	for _, workingDirectory := range []string{"../outside", "/absolute", "pkg/../work", "pkg//work", ".", "nul\x00byte"} {
		test.Run(workingDirectory, func(test *testing.T) {
			creator := mock.NewMockBuildDirectoryCreator(gomock.NewController(test))
			_, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, workingDirectory, nil, nil)
			require.Error(test, err)
		})
	}
}

func TestPersistentWorkerWorkspaceInvalidToolPaths(test *testing.T) {
	for _, toolPaths := range [][]string{{"../outside"}, {"/absolute"}, {"tools/../compiler"}, {"nul\x00byte"}, {"work"}, {"tools", "tools/compiler"}} {
		test.Run(toolPaths[0], func(test *testing.T) {
			creator := mock.NewMockBuildDirectoryCreator(gomock.NewController(test))
			_, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "work", toolPaths, nil)
			require.Error(test, err)
		})
	}
}

func TestPersistentWorkerWorkspaceWorkingDirectoryReplaced(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID), pool.EmptyFilePool, "work", nil, nil)
	require.NoError(test, err)
	inputDigest := storage.addDirectory(&remoteexecution.Directory{})
	outputs := persistentWorkspaceTestOutputs(test, "work")
	lease, err := workspace.Prepare(context.Background(), inputDigest, outputs)
	require.NoError(test, err)
	lease.Release()
	workingPath := filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString(), "root/work")
	require.NoError(test, os.RemoveAll(workingPath))
	outsidePath := test.TempDir()
	require.NoError(test, os.WriteFile(filepath.Join(outsidePath, "keep"), []byte("safe"), 0o600))
	require.NoError(test, os.Symlink(outsidePath, workingPath))
	_, err = workspace.Prepare(context.Background(), inputDigest, outputs)
	require.Error(test, err)
	_, err = os.Stat(filepath.Join(outsidePath, "keep"))
	require.NoError(test, err)
	require.NoError(test, workspace.Close(context.Background(), func(context.Context) error { return nil }))
}

func TestPersistentWorkerWorkspaceFailedPreparation(test *testing.T) {
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID), pool.EmptyFilePool, "", nil, nil)
	require.NoError(test, err)
	missingDirectory := storage.addBytes([]byte("missing directory"))
	_, err = workspace.Prepare(context.Background(), missingDirectory, persistentWorkspaceTestOutputs(test, ""))
	require.Error(test, err)
	_, err = workspace.Prepare(context.Background(), storage.addDirectory(&remoteexecution.Directory{}), persistentWorkspaceTestOutputs(test, ""))
	require.Error(test, err)
	_, err = os.Stat(filepath.Join(nativePath, workspace.GetBuildDirectoryPath().GetUNIXString()))
	require.NoError(test, err)
	require.NoError(test, workspace.Close(context.Background(), func(context.Context) error { return nil }))
}

func TestPersistentWorkerWorkspaceVirtualLifetime(test *testing.T) {
	for _, allocatorType := range []string{"FUSE", "NFS"} {
		test.Run(allocatorType, func(test *testing.T) {
			storage := newPersistentWorkspaceTestStorage(test)
			var allocator virtual.StatefulHandleAllocator
			if allocatorType == "FUSE" {
				allocator = virtual.NewFUSEHandleAllocator(random.FastThreadSafeGenerator)
			} else {
				allocator = virtual.NewNFSHandleAllocator(random.NewFastSingleThreadedGenerator())
			}
			setAttributes := func(virtual.AttributesMask, *virtual.Attributes) {}
			symlinks := virtual.NewHandleAllocatingSymlinkFactory(virtual.NewBaseSymlinkFactory(setAttributes), allocator.New(), path.UNIXFormat)
			devices := virtual.NewHandleAllocatingCharacterDeviceFactory(virtual.BaseCharacterDeviceFactory, allocator.New())
			rootDirectory := virtual.NewInMemoryPrepopulatedDirectory(nil, symlinks, nil, allocator, sort.Sort, func(string) bool { return false }, clock.SystemClock, virtual.CaseSensitiveComponentNormalizer, setAttributes, virtual.NoNamedAttributesFactory)
			buildDirectory := builder.NewVirtualBuildDirectory(rootDirectory, storage.fetcher, storage.blobs, symlinks, devices, allocator, setAttributes, clock.SystemClock)
			ctrl := gomock.NewController(test)
			filePool := mock.NewMockFilePool(ctrl)
			cacheFile := mock.NewMockFileReadWriter(ctrl)
			filePool.EXPECT().NewFile(pool.ZeroHoleSource, uint64(0)).Return(cacheFile, nil)
			cacheFile.EXPECT().WriteAt([]byte("cache"), int64(0)).Return(5, nil)
			cacheFile.EXPECT().Close().Return(nil)
			var nextID atomic.Uint64
			workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(buildDirectory), &nextID), pool.NewQuotaEnforcingFilePool(filePool, 1, 10), "work", []string{"work/compiler"}, map[path.Component]filesystem.DeviceNumber{path.MustNewComponent("null"): filesystem.NewDeviceNumberFromMajorMinor(1, 3)})
			require.NoError(test, err)
			defer func() {
				require.NoError(test, workspace.Close(context.Background(), func(context.Context) error { return nil }))
			}()
			fileDigest := storage.addBytes([]byte("retained contents"))
			toolFile := &remoteexecution.FileNode{Name: "compiler", Digest: storage.addBytes([]byte("compiler")).GetProto(), IsExecutable: true}
			workDigest := storage.addDirectory(&remoteexecution.Directory{Files: []*remoteexecution.FileNode{toolFile, {Name: "source", Digest: fileDigest.GetProto()}}})
			rootDigest := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "work", Digest: workDigest.GetProto()}, {Name: "lazy", Digest: workDigest.GetProto()}}})
			firstContext, cancelFirst := context.WithCancel(context.Background())
			defer cancelFirst()
			outputs := persistentWorkspaceTestOutputs(test, "work")
			firstLease, err := workspace.Prepare(firstContext, rootDigest, outputs)
			require.NoError(test, err)
			sessionChild, err := rootDirectory.LookupChild(path.MustNewComponent(workspace.GetBuildDirectoryPath().GetUNIXString()))
			require.NoError(test, err)
			sessionDirectory, _ := sessionChild.GetPair()
			temporaryChild, err := sessionDirectory.LookupChild(path.MustNewComponent("tmp"))
			require.NoError(test, err)
			temporaryDirectory, _ := temporaryChild.GetPair()
			var attributes virtual.Attributes
			cacheLeaf, _, _, openStatus := temporaryDirectory.VirtualOpenChild(context.Background(), path.MustNewComponent("cache"), virtual.ShareMaskWrite, &virtual.Attributes{}, nil, 0, &attributes)
			require.Equal(test, virtual.StatusOK, openStatus)
			defer cacheLeaf.VirtualClose(virtual.ShareMaskWrite)
			inputChild, err := sessionDirectory.LookupChild(path.MustNewComponent("root"))
			require.NoError(test, err)
			inputDirectory, _ := inputChild.GetPair()
			workChild, err := inputDirectory.LookupChild(path.MustNewComponent("work"))
			require.NoError(test, err)
			workDirectory, _ := workChild.GetPair()
			toolChild, err := workDirectory.LookupChild(path.MustNewComponent("compiler"))
			require.NoError(test, err)
			_, retainedTool := toolChild.GetPair()
			sourceChild, err := workDirectory.LookupChild(path.MustNewComponent("source"))
			require.NoError(test, err)
			_, retainedSource := sourceChild.GetPair()
			firstLease.Release()
			cancelFirst()
			readBuffer := make([]byte, 32)
			readCount, _, readStatus := retainedSource.VirtualRead(context.Background(), readBuffer, 0)
			require.Equal(test, virtual.StatusOK, readStatus)
			require.Equal(test, "retained contents", string(readBuffer[:readCount]))
			lazyChild, err := inputDirectory.LookupChild(path.MustNewComponent("lazy"))
			require.NoError(test, err)
			lazyDirectory, _ := lazyChild.GetPair()
			_, err = lazyDirectory.LookupChild(path.MustNewComponent("source"))
			require.NoError(test, err)

			newFileDigest := storage.addBytes([]byte("new contents"))
			newWork := storage.addDirectory(&remoteexecution.Directory{Files: []*remoteexecution.FileNode{toolFile, {Name: "source", Digest: newFileDigest.GetProto()}}})
			newRoot := storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{{Name: "work", Digest: newWork.GetProto()}}})
			secondLease, err := workspace.Prepare(context.Background(), newRoot, outputs)
			require.NoError(test, err)
			defer secondLease.Release()
			written, writeStatus := cacheLeaf.VirtualWrite(context.Background(), []byte("cache"), 0)
			require.Equal(test, virtual.StatusOK, writeStatus)
			require.Equal(test, 5, written)
			newWorkChild, err := inputDirectory.LookupChild(path.MustNewComponent("work"))
			require.NoError(test, err)
			newWorkDirectory, _ := newWorkChild.GetPair()
			require.Same(test, workDirectory, newWorkDirectory)
			newToolChild, err := newWorkDirectory.LookupChild(path.MustNewComponent("compiler"))
			require.NoError(test, err)
			_, newTool := newToolChild.GetPair()
			require.Same(test, retainedTool, newTool)
			newSourceChild, err := newWorkDirectory.LookupChild(path.MustNewComponent("source"))
			require.NoError(test, err)
			_, newSource := newSourceChild.GetPair()
			readCount, _, readStatus = newSource.VirtualRead(context.Background(), readBuffer, 0)
			require.Equal(test, virtual.StatusOK, readStatus)
			require.Equal(test, "new contents", string(readBuffer[:readCount]))
			_, err = inputDirectory.LookupChild(path.MustNewComponent("lazy"))
			require.True(test, os.IsNotExist(err))
			delete(storage.contents, fileDigest)
			_, _, readStatus = retainedSource.VirtualRead(context.Background(), readBuffer, 0)
			require.Equal(test, virtual.StatusErrIO, readStatus)
			<-secondLease.Context.Done()
			require.Error(test, workspace.GetIOError())
			secondLease.Release()
			_, err = workspace.Prepare(context.Background(), newRoot, outputs)
			require.Error(test, err)
		})
	}
}

type persistentWorkspaceHookDirectory struct {
	builder.BuildDirectory
	logger util.ErrorLogger
}

func (directory *persistentWorkspaceHookDirectory) InstallHooks(filePool pool.FilePool, logger util.ErrorLogger) {
	directory.logger = logger
	directory.BuildDirectory.InstallHooks(filePool, logger)
}

func TestPersistentWorkerWorkspaceCancellationAndIdleIOError(test *testing.T) {
	for _, idleError := range []bool{false, true} {
		test.Run(map[bool]string{false: "Cancellation", true: "IdleIOError"}[idleError], func(test *testing.T) {
			storage := newPersistentWorkspaceTestStorage(test)
			rootDirectory, _ := storage.newNativeRoot()
			var nextID atomic.Uint64
			baseCreator := builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID)
			directory, buildPath, err := baseCreator.GetBuildDirectory(context.Background(), nil)
			require.NoError(test, err)
			hooks := &persistentWorkspaceHookDirectory{BuildDirectory: directory}
			creator := mock.NewMockBuildDirectoryCreator(gomock.NewController(test))
			creator.EXPECT().GetBuildDirectory(gomock.Any(), nil).Return(hooks, buildPath, nil)
			workspace, err := builder.NewPersistentWorkerWorkspace(context.Background(), creator, pool.EmptyFilePool, "", nil, nil)
			require.NoError(test, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inputDigest := storage.addDirectory(&remoteexecution.Directory{})
			outputs := persistentWorkspaceTestOutputs(test, "")
			lease, err := workspace.Prepare(ctx, inputDigest, outputs)
			require.NoError(test, err)
			if idleError {
				lease.Release()
				hooks.logger.Log(status.Error(codes.DataLoss, "Idle read failed"))
				require.Error(test, workspace.GetIOError())
			} else {
				cancel()
				<-lease.Context.Done()
				lease.Release()
			}
			_, err = workspace.Prepare(context.Background(), inputDigest, outputs)
			require.Error(test, err)
			require.NoError(test, workspace.Close(context.Background(), func(cleanupContext context.Context) error {
				require.NoError(test, cleanupContext.Err())
				return nil
			}))
		})
	}
}
