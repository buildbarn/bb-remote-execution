package virtual_test

import (
	"context"
	"io"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
)

func TestPoolBackedFileAllocatorGetBazelOutputServiceStat(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Create a file and initialize it with some contents.
	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskRead|virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	underlyingFile.EXPECT().WriteAt([]byte("Hello"), int64(0)).Return(5, nil)
	n, s := f.VirtualWrite([]byte("Hello"), 0)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, 5, n)

	// When the file is opened for writing, we should not report the
	// file's digest, even if it is requested. There is no way we
	// can compute its value properly, as unwritten data may still
	// be present in the page cache.
	digestFunction1 := digest.MustNewFunction("Hello", remoteexecution.DigestFunction_MD5)
	p := virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction1,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{},
		},
	}, p.Stat)

	f.VirtualClose(virtual.ShareMaskWrite)

	// The digest of the file should be computed on demand. This is
	// more efficient than letting the build client read the file
	// through the FUSE file system.
	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
	p = virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction1,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	locator, err := anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
		Digest: &remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 5,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, p.Stat)

	// Calling the function a second time should not generate any
	// reads against the file, as the contents of the file have not
	// changed. A cached value should be returned.
	p = virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction1,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, p.Stat)

	// Change the file's contents to invalidate the cached digest. A
	// successive call to ApplyGetBazelOutputServiceStat
	// should recompute the digest.
	require.Equal(t, virtual.StatusOK, f.VirtualOpenSelf(ctx, virtual.ShareMaskWrite, &virtual.OpenExistingOptions{}, 0, &virtual.Attributes{}))
	underlyingFile.EXPECT().WriteAt([]byte(" world"), int64(5)).Return(6, nil)
	n, s = f.VirtualWrite([]byte(" world"), 5)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, 6, n)
	f.VirtualClose(virtual.ShareMaskWrite)

	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello world"), io.EOF
		})
	p = virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction1,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	locator, err = anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
		Digest: &remoteexecution.Digest{
			Hash:      "3e25960a79dbc69b674cd4ec67a72c62",
			SizeBytes: 11,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, p.Stat)

	// The cached digest should be ignored in case the instance name
	// or hashing function is changed.
	underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, "Hello world"), io.EOF
		})
	digestFunction2 := digest.MustNewFunction("Hello", remoteexecution.DigestFunction_SHA256)
	p = virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction2,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	locator, err = anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
		Digest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, p.Stat)

	// Once a cached digest is present, it should also become part
	// of output path persistent state file.
	var directory outputpathpersistency.Directory
	pAppend := virtual.ApplyAppendOutputPathPersistencyDirectoryNode{
		Directory: &directory,
		Name:      path.MustNewComponent("hello.txt"),
	}
	require.True(t, f.VirtualApply(&pAppend))
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
	f.VirtualClose(virtual.ShareMaskRead)
}

// For plain lseek() operations such as SEEK_SET, SEEK_CUR and SEEK_END,
// the kernel never calls into userspace, as the kernel is capable of
// handling those requests directly. However, For SEEK_HOLE and
// SEEK_DATA, the kernel does create calls, as the kernel is unaware of
// which parts of the file contain holes.
func TestPoolBackedFileAllocatorVirtualSeek(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskRead|virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	// Grow the file.
	underlyingFile.EXPECT().Truncate(int64(1000))

	require.Equal(t, virtual.StatusOK, f.VirtualSetAttributes(
		ctx,
		(&virtual.Attributes{}).SetSizeBytes(1000),
		0,
		&virtual.Attributes{}))

	t.Run("Failure", func(t *testing.T) {
		// I/O errors on the file should be captured.
		underlyingFile.EXPECT().GetNextRegionOffset(int64(123), filesystem.Data).
			Return(int64(0), status.Error(codes.Internal, "Disk on fire"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to get next region offset at offset 123: Disk on fire")))

		_, s := f.VirtualSeek(123, filesystem.Data)
		require.Equal(t, virtual.StatusErrIO, s)
	})

	t.Run("AtEndOfFile", func(t *testing.T) {
		// End-of-file errors should be converted to ENXIO, as
		// described in the lseek() manual page.
		_, s := f.VirtualSeek(1000, filesystem.Hole)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})

	t.Run("PastEndOfFile", func(t *testing.T) {
		_, s := f.VirtualSeek(1001, filesystem.Hole)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})

	t.Run("SuccessData", func(t *testing.T) {
		underlyingFile.EXPECT().GetNextRegionOffset(int64(789), filesystem.Data).
			Return(int64(790), nil)

		offset, s := f.VirtualSeek(789, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(790), *offset)
	})

	t.Run("SuccessFinalHole", func(t *testing.T) {
		underlyingFile.EXPECT().GetNextRegionOffset(int64(912), filesystem.Data).
			Return(int64(0), io.EOF)

		offset, s := f.VirtualSeek(912, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Nil(t, offset)
	})
}

// Removal of files through the filesystem.Directory interface will not
// update the name cache of go-virtual. References to inodes may continue
// to exist after inodes are removed from the directory hierarchy. This
// could cause go-fuse to call Open() on a file that is already closed.
// Nothing bad should happen when this occurs.
func TestPoolBackedFileAllocatorVirtualOpenSelfStaleAfterUnlink(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Close()
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	f.VirtualClose(virtual.ShareMaskWrite)
	f.Unlink()

	require.Equal(
		t,
		virtual.StatusErrStale,
		f.VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, 0, &virtual.Attributes{}))
}

// This test is the same as the above, except that the file reference
// count drops from one to zero due to Release() (i.e., file descriptor
// closure), as opposed to Unlink().
func TestPoolBackedFileAllocatorVirtualOpenSelfStaleAfterClose(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Close()
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	f.Unlink()
	f.VirtualClose(virtual.ShareMaskWrite)

	require.Equal(
		t,
		virtual.StatusErrStale,
		f.VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, 0, &virtual.Attributes{}))
}

func TestPoolBackedFileAllocatorVirtualRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskRead|virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	// Let initial tests assume an empty file.
	t.Run("EmptyFileAtStart", func(t *testing.T) {
		var p [10]byte
		n, eof, s := f.VirtualRead(p[:], 0)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, 0, n)
		require.True(t, eof)
	})

	t.Run("EmptyFilePastEnd", func(t *testing.T) {
		var p [10]byte
		n, eof, s := f.VirtualRead(p[:], 10)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, 0, n)
		require.True(t, eof)
	})

	// Let the remainder of the tests assume a non-empty file.
	underlyingFile.EXPECT().WriteAt([]byte("Hello"), int64(0)).Return(5, nil)
	n, s := f.VirtualWrite([]byte("Hello"), 0)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, 5, n)

	t.Run("IOFailure", func(t *testing.T) {
		// Read errors should be converted to EIO errors. In
		// order to capture error details, the underlying error
		// is forwarded to an error logger.
		underlyingFile.EXPECT().ReadAt(gomock.Len(3), int64(2)).
			Return(0, status.Error(codes.Unavailable, "Storage backends offline"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Unavailable, "Failed to read from file at offset 2: Storage backends offline")))

		var p [10]byte
		_, _, s := f.VirtualRead(p[:], 2)
		require.Equal(t, virtual.StatusErrIO, s)
	})

	t.Run("EOF", func(t *testing.T) {
		// Read EOF errors should not be converted to EIO
		// errors. They should simply be translated to
		// go_fuse.OK, as POSIX read() returns zero to indicate
		// end-of-file.
		underlyingFile.EXPECT().ReadAt(gomock.Len(3), int64(2)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				return copy(p, "llo"), io.EOF
			})

		var p [10]byte
		n, eof, s := f.VirtualRead(p[:], 2)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, 3, n)
		require.True(t, eof)
		require.Equal(t, []byte("llo"), p[:3])
	})

	underlyingFile.EXPECT().Close()

	f.VirtualClose(virtual.ShareMaskRead | virtual.ShareMaskWrite)
	f.Unlink()
}

// Truncation errors should be converted to EIO errors. In order to
// capture error details, the underlying error is forwarded to an error
// logger.
func TestPoolBackedFileAllocatorFUSETruncateFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	underlyingFile.EXPECT().Truncate(int64(42)).Return(status.Error(codes.Unavailable, "Storage backends offline"))
	underlyingFile.EXPECT().Close()

	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Unavailable, "Failed to truncate file to length 42: Storage backends offline")))

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	require.Equal(t, virtual.StatusErrIO, f.VirtualSetAttributes(
		ctx,
		(&virtual.Attributes{}).SetSizeBytes(42),
		0,
		&virtual.Attributes{}))
	f.VirtualClose(virtual.ShareMaskWrite)
	f.Unlink()
}

// Write errors should be converted to EIO errors. In order to capture
// error details, the underlying error is forwarded to an error logger.
func TestPoolBackedFileAllocatorVirtualWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	var p [10]byte
	underlyingFile.EXPECT().WriteAt(p[:], int64(42)).Return(0, status.Error(codes.Unavailable, "Storage backends offline"))
	underlyingFile.EXPECT().Close()

	errorLogger := mock.NewMockErrorLogger(ctrl)
	errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Unavailable, "Failed to write to file at offset 42: Storage backends offline")))

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)
	_, s = f.VirtualWrite(p[:], 42)
	require.Equal(t, virtual.StatusErrIO, s)
	f.VirtualClose(virtual.ShareMaskWrite)
	f.Unlink()
}

func TestPoolBackedFileAllocatorUploadFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Create a file backed by a FilePool.
	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	// Initialize the file with the contents "Hello".
	underlyingFile.EXPECT().WriteAt([]byte("Hello"), int64(0)).Return(5, nil)
	n, s := f.VirtualWrite([]byte("Hello"), 0)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, 5, n)

	fileDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digestFunction := fileDigest.GetDigestFunction()
	writableFileUploadDelay := make(chan struct{})
	close(writableFileUploadDelay)

	t.Run("DigestComputationIOFailure", func(t *testing.T) {
		underlyingFile.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, syscall.EIO)
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

		p := virtual.ApplyUploadFile{
			Context:                   ctx,
			ContentAddressableStorage: contentAddressableStorage,
			DigestFunction:            digestFunction,
			WritableFileUploadDelay:   writableFileUploadDelay,
		}
		require.True(t, f.VirtualApply(&p))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to compute file digest: input/output error"), p.Err)
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

		p := virtual.ApplyUploadFile{
			Context:                   ctx,
			ContentAddressableStorage: contentAddressableStorage,
			DigestFunction:            digestFunction,
			WritableFileUploadDelay:   writableFileUploadDelay,
		}
		require.True(t, f.VirtualApply(&p))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to upload file: Server on fire"), p.Err)
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
					require.Equal(t, virtual.StatusOK, f.VirtualAllocate(100, 23))
					close(a1)
				}()

				a2 := make(chan struct{})
				go func() {
					require.Equal(t, virtual.StatusOK, f.VirtualSetAttributes(
						ctx,
						(&virtual.Attributes{}).SetSizeBytes(123),
						0,
						&virtual.Attributes{}))
					close(a2)
				}()

				a3 := make(chan struct{})
				go func() {
					require.Equal(t, virtual.StatusOK, f.VirtualOpenSelf(
						ctx,
						virtual.ShareMaskWrite,
						&virtual.OpenExistingOptions{
							Truncate: true,
						},
						0,
						&virtual.Attributes{}))
					f.VirtualClose(virtual.ShareMaskWrite)
					close(a3)
				}()

				a4 := make(chan struct{})
				go func() {
					n, s := f.VirtualWrite([]byte("Foo"), 120)
					require.Equal(t, virtual.StatusOK, s)
					require.Equal(t, 3, n)
					close(a4)
				}()

				// Even though VirtualSetAttributes()
				// with a size (truncate()) should
				// block, it is perfectly fine to change
				// the file's permissions.
				require.Equal(t, virtual.StatusOK, f.VirtualSetAttributes(
					ctx,
					(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite|virtual.PermissionsExecute),
					0,
					&virtual.Attributes{}))

				underlyingFile.EXPECT().Truncate(int64(123)).MinTimes(1).MaxTimes(2)
				underlyingFile.EXPECT().Truncate(int64(0))
				underlyingFile.EXPECT().WriteAt([]byte("Foo"), int64(120)).Return(3, nil)

				// Complete reading the file.
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)

				// All mutable operations should now be
				// able to complete.
				<-a1
				<-a2
				<-a3
				<-a4
				return nil
			})

		p := virtual.ApplyUploadFile{
			Context:                   ctx,
			ContentAddressableStorage: contentAddressableStorage,
			DigestFunction:            digestFunction,
			WritableFileUploadDelay:   writableFileUploadDelay,
		}
		require.True(t, f.VirtualApply(&p))
		require.NoError(t, p.Err)
		require.Equal(t, fileDigest, p.Digest)
	})

	underlyingFile.EXPECT().Close()
	f.VirtualClose(virtual.ShareMaskWrite)
	f.Unlink()

	t.Run("Stale", func(t *testing.T) {
		contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

		// Uploading a file that has already been released
		// should fail. It should not cause accidental access to
		// the closed file handle.
		p := virtual.ApplyUploadFile{
			Context:                   ctx,
			ContentAddressableStorage: contentAddressableStorage,
			DigestFunction:            digestFunction,
			WritableFileUploadDelay:   writableFileUploadDelay,
		}
		require.True(t, f.VirtualApply(&p))
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "File was unlinked before uploading could start"), p.Err)
	})
}

func TestPoolBackedFileAllocatorVirtualClose(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Create a new file.
	pool := mock.NewMockFilePool(ctrl)
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	pool.EXPECT().NewFile().Return(underlyingFile, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	f, s := virtual.NewPoolBackedFileAllocator(pool, errorLogger).
		NewFile(false, 0, virtual.ShareMaskWrite)
	require.Equal(t, virtual.StatusOK, s)

	// Initially it should be opened exactly once. Open it a couple
	// more times.
	for i := 0; i < 10; i++ {
		require.Equal(
			t,
			virtual.StatusOK,
			f.VirtualOpenSelf(
				ctx,
				virtual.ShareMaskRead,
				&virtual.OpenExistingOptions{},
				0,
				&virtual.Attributes{}))
	}
	for i := 0; i < 10; i++ {
		require.Equal(
			t,
			virtual.StatusOK,
			f.VirtualOpenSelf(
				ctx,
				virtual.ShareMaskRead|virtual.ShareMaskWrite,
				&virtual.OpenExistingOptions{},
				0,
				&virtual.Attributes{}))
	}

	// Unlinking the file should not cause the underlying file to be
	// released, as it's opened.
	f.Unlink()

	// The underlying file should be released only when the close
	// count matches the number of times the file was opened.
	for i := 0; i < 10; i++ {
		f.VirtualClose(virtual.ShareMaskRead)
	}
	for i := 0; i < 10; i++ {
		f.VirtualClose(virtual.ShareMaskRead | virtual.ShareMaskWrite)
	}
	for i := 0; i < 100; i++ {
		f.VirtualClose(0)
	}
	underlyingFile.EXPECT().Close()
	f.VirtualClose(virtual.ShareMaskWrite)
}
