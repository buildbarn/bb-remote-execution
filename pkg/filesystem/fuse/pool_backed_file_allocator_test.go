//go:build darwin || linux
// +build darwin linux

package fuse_test

import (
	"context"
	"io"
	"os"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPoolBackedFileAllocatorGetOutputServiceFileStatus(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Create a file and initialize it with some contents.
	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	underlyingFile.EXPECT().WriteAt([]byte("Hello"), int64(0)).Return(5, nil)
	n, s := f.FUSEWrite([]byte("Hello"), 0)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, uint32(5), n)

	// When the provided digest.Function is nil, we should only
	// report that this is a file.
	fileStatus, err := f.GetOutputServiceFileStatus(nil)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{},
		},
	}, fileStatus)

	// When the provided digest.Function is set, the digest of the
	// file should be computed on demand. This is more efficient
	// than letting the build client read the file through the FUSE
	// file system.
	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
	digestFunction1 := digest.MustNewFunction("Hello", remoteexecution.DigestFunction_MD5)
	fileStatus, err = f.GetOutputServiceFileStatus(&digestFunction1)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{
				Digest: &remoteexecution.Digest{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 5,
				},
			},
		},
	}, fileStatus)

	// Calling the function a second time should not generate any
	// reads against the file, as the contents of the file have not
	// changed. A cached value should be returned.
	fileStatus, err = f.GetOutputServiceFileStatus(&digestFunction1)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{
				Digest: &remoteexecution.Digest{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 5,
				},
			},
		},
	}, fileStatus)

	// Change the file's contents to invalidate the cached digest. A
	// successive call to GetOutputServiceFileStatus() should
	// recompute the digest.
	underlyingFile.EXPECT().WriteAt([]byte(" world"), int64(5)).Return(6, nil)
	n, s = f.FUSEWrite([]byte(" world"), 5)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, uint32(6), n)

	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello world"), io.EOF
		})
	fileStatus, err = f.GetOutputServiceFileStatus(&digestFunction1)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{
				Digest: &remoteexecution.Digest{
					Hash:      "3e25960a79dbc69b674cd4ec67a72c62",
					SizeBytes: 11,
				},
			},
		},
	}, fileStatus)

	// The cached digest should be ignored in case the instance name
	// or hashing function is changed.
	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello world"), io.EOF
		})
	digestFunction2 := digest.MustNewFunction("Hello", remoteexecution.DigestFunction_SHA256)
	fileStatus, err = f.GetOutputServiceFileStatus(&digestFunction2)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{
				Digest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
			},
		},
	}, fileStatus)

	// Once a cached digest is present, it should also become part
	// of output path persistent state file.
	var directory outputpathpersistency.Directory
	f.AppendOutputPathPersistencyDirectoryNode(&directory, path.MustNewComponent("hello.txt"))
	testutil.RequireEqualProto(t, &outputpathpersistency.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "hello.txt",
				Digest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
			},
		},
	}, &directory)

	underlyingFile.EXPECT().Close()
	f.Unlink()
	f.FUSERelease()
}

// For plain lseek() operations such as SEEK_SET, SEEK_CUR and SEEK_END,
// the kernel never calls into userspace, as the kernel is capable of
// handling those requests directly. However, For SEEK_HOLE and
// SEEK_DATA, the kernel does create calls, as the kernel is unaware of
// which parts of the file contain holes.
func TestPoolBackedFileAllocatorFUSELseek(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	t.Run("Failure", func(t *testing.T) {
		// I/O errors on the file should be captured.
		underlyingFile.EXPECT().GetNextRegionOffset(int64(123), filesystem.Data).
			Return(int64(0), status.Error(codes.Internal, "Disk on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to get next region offset at offset 123: Disk on fire"))

		var out go_fuse.LseekOut
		require.Equal(t, go_fuse.EIO, f.FUSELseek(&go_fuse.LseekIn{
			Offset: 123,
			Whence: unix.SEEK_DATA,
		}, &out))
	})

	t.Run("EndOfFile", func(t *testing.T) {
		// End-of-file errors should be converted to ENXIO, as
		// described in the lseek() manual page.
		underlyingFile.EXPECT().GetNextRegionOffset(int64(456), filesystem.Hole).
			Return(int64(0), io.EOF)

		var out go_fuse.LseekOut
		require.Equal(t, go_fuse.Status(syscall.ENXIO), f.FUSELseek(&go_fuse.LseekIn{
			Offset: 456,
			Whence: unix.SEEK_HOLE,
		}, &out))
	})

	t.Run("Success", func(t *testing.T) {
		underlyingFile.EXPECT().GetNextRegionOffset(int64(789), filesystem.Data).
			Return(int64(790), nil)

		var out go_fuse.LseekOut
		require.Equal(t, go_fuse.OK, f.FUSELseek(&go_fuse.LseekIn{
			Offset: 789,
			Whence: unix.SEEK_DATA,
		}, &out))
		require.Equal(t, go_fuse.LseekOut{Offset: 790}, out)
	})
}

// Removal of files through the filesystem.Directory interface will not
// update the name cache of go-fuse. References to inodes may continue
// to exist after inodes are removed from the directory hierarchy. This
// could cause go-fuse to call Open() on a file that is already closed.
// Nothing bad should happen when this occurs.
func TestPoolBackedFileAllocatorFUSEOpenStaleAfterUnlink(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Close()
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	f.FUSERelease()
	f.Unlink()

	require.Equal(t, go_fuse.Status(syscall.ESTALE), f.FUSEOpen(0))
}

// This test is the same as the above, except that the file reference
// count drops from one to zero due to Release() (i.e., file descriptor
// closure), as opposed to Unlink().
func TestPoolBackedFileAllocatorFUSEOpenStaleAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Close()
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	f.Unlink()
	f.FUSERelease()

	require.Equal(t, go_fuse.Status(syscall.ESTALE), f.FUSEOpen(0))
}

// Read errors should be converted to EIO errors. In order to capture
// error details, the underlying error is forwarded to an error logger.
func TestPoolBackedFileAllocatorFUSEReadFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	var p [10]byte
	underlyingFile.EXPECT().ReadAt(p[:], int64(42)).Return(0, status.Error(codes.Unavailable, "Storage backends offline"))
	underlyingFile.EXPECT().Close()

	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Failed to read from file at offset 42: Storage backends offline"))

	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	_, s = f.FUSERead(p[:], 42)
	require.Equal(t, s, go_fuse.EIO)
	f.FUSERelease()
	f.Unlink()
}

// Read EOF errors should not be converted to EIO errors. They should
// simply be translated to go_fuse.OK, as POSIX read() returns zero to
// indicate end-of-file.
func TestPoolBackedFileAllocatorFUSEReadEOF(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	var p [10]byte
	underlyingFile.EXPECT().ReadAt(p[:], int64(42)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
	underlyingFile.EXPECT().Close()
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	r, s := f.FUSERead(p[:], 42)
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, 5, r.Size())
	f.FUSERelease()
	f.Unlink()
}

// Truncation errors should be converted to EIO errors. In order to
// capture error details, the underlying error is forwarded to an error
// logger.
func TestPoolBackedFileAllocatorFUSETruncateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Truncate(int64(42)).Return(status.Error(codes.Unavailable, "Storage backends offline"))
	underlyingFile.EXPECT().Close()

	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Failed to truncate file to length 42: Storage backends offline"))

	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	var setAttrIn go_fuse.SetAttrIn
	setAttrIn.Valid = go_fuse.FATTR_SIZE
	setAttrIn.Size = 42
	var attr go_fuse.Attr
	require.Equal(t, go_fuse.EIO, f.FUSESetAttr(&setAttrIn, &attr))
	f.FUSERelease()
	f.Unlink()
}

// Write errors should be converted to EIO errors. In order to capture
// error details, the underlying error is forwarded to an error logger.
func TestPoolBackedFileAllocatorFUSEWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	var p [10]byte
	underlyingFile.EXPECT().WriteAt(p[:], int64(42)).Return(0, status.Error(codes.Unavailable, "Storage backends offline"))
	underlyingFile.EXPECT().Close()

	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Failed to write to file at offset 42: Storage backends offline"))

	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)
	_, s = f.FUSEWrite(p[:], 42)
	require.Equal(t, s, go_fuse.EIO)
	f.FUSERelease()
	f.Unlink()
}

func TestPoolBackedFileAllocatorFUSEUploadFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Create a file backed by a FilePool.
	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	inodeNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	inodeNumberGenerator.EXPECT().Uint64().Return(uint64(42))

	f, s := fuse.NewPoolBackedFileAllocator(pool, errorLogger, inodeNumberGenerator).
		NewFile(uint32(os.O_RDWR), 0o666)
	require.Equal(t, go_fuse.OK, s)

	// Initialize the file with the contents "Hello".
	underlyingFile.EXPECT().WriteAt([]byte("Hello"), int64(0)).Return(5, nil)
	n, s := f.FUSEWrite([]byte("Hello"), 0)
	require.Equal(t, uint32(5), n)
	require.Equal(t, go_fuse.OK, s)

	fileDigest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)
	digestFunction := fileDigest.GetDigestFunction()

	t.Run("DigestComputationIOFailure", func(t *testing.T) {
		underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, syscall.EIO)
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

		_, err := f.UploadFile(ctx, contentAddressableStorage, digestFunction)
		require.Equal(t, status.Error(codes.Internal, "Failed to compute file digest: input/output error"), err)
	})

	t.Run("UploadFailure", func(t *testing.T) {
		underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, io.EOF
		})
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
		contentAddressableStorage.EXPECT().Put(ctx, fileDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		_, err := f.UploadFile(ctx, contentAddressableStorage, digestFunction)
		require.Equal(t, status.Error(codes.Internal, "Failed to upload file: Server on fire"), err)
	})

	t.Run("Success", func(t *testing.T) {
		underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, io.EOF
		})
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
		contentAddressableStorage.EXPECT().Put(ctx, fileDigest, gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				// As long as we haven't completely read
				// the file, any operation that modifies
				// the file's contents should block.
				// Tests for affected operations below.
				a1 := make(chan struct{})
				go func() {
					require.Equal(t, go_fuse.OK, f.FUSEFallocate(1, 1))
					close(a1)
				}()

				a2 := make(chan struct{})
				go func() {
					require.Equal(t, go_fuse.OK, f.FUSESetAttr(&go_fuse.SetAttrIn{
						SetAttrInCommon: go_fuse.SetAttrInCommon{
							Valid: go_fuse.FATTR_SIZE,
							Size:  123,
						},
					}, &go_fuse.Attr{}))
					close(a2)
				}()

				a3 := make(chan struct{})
				go func() {
					n, s := f.FUSEWrite([]byte("Foo"), 123)
					require.Equal(t, uint32(3), n)
					require.Equal(t, go_fuse.OK, s)
					close(a3)
				}()

				// Even though FUSESetAttr() with
				// FATTR_SIZE (truncate()) should block,
				// it is perfectly fine to change the
				// file's mode.
				require.Equal(t, go_fuse.OK, f.FUSESetAttr(&go_fuse.SetAttrIn{
					SetAttrInCommon: go_fuse.SetAttrInCommon{
						Valid: go_fuse.FATTR_MODE,
						Mode:  0o777,
					},
				}, &go_fuse.Attr{}))

				underlyingFile.EXPECT().Truncate(int64(123)).Times(1)
				underlyingFile.EXPECT().WriteAt([]byte("Foo"), gomock.Any()).Return(3, nil)

				// Complete reading the file.
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)

				// All mutable operations should now be
				// able to complete.
				<-a1
				<-a2
				<-a3
				return nil
			})

		uploadedDigest, err := f.UploadFile(ctx, contentAddressableStorage, digestFunction)
		require.NoError(t, err)
		require.Equal(t, fileDigest, uploadedDigest)
	})

	underlyingFile.EXPECT().Close()
	f.FUSERelease()
	f.Unlink()

	t.Run("Stale", func(t *testing.T) {
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

		// Uploading a file that has already been released
		// should fail. It should not cause accidental access to
		// the closed file handle.
		_, err := f.UploadFile(ctx, contentAddressableStorage, digestFunction)
		require.Equal(t, status.Error(codes.NotFound, "File was unlinked before uploading could start"), err)
	})
}
