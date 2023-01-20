package builder_test

import (
	"context"
	"io"
	"os"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNaiveBuildDirectorySuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "directory",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
		Files: []*remoteexecution.FileNode{
			{
				Name: "non-executable",
				Digest: &remoteexecution.Digest{
					Hash:      "9999999999999999999999999999999999999999999999999999999999999999",
					SizeBytes: 512,
				},
			},
			{
				Name: "executable",
				Digest: &remoteexecution.Digest{
					Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SizeBytes: 512,
				},
				IsExecutable: true,
			},
		},
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "link-to-executable",
				Target: "executable",
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "link-to-non-executable",
				Target: "../non-executable",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("directory"), os.FileMode(0o777)).Return(nil)
	nestedDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("directory")).Return(nestedDirectory, nil)
	nestedDirectory.EXPECT().Symlink("../non-executable", path.MustNewComponent("link-to-non-executable")).Return(nil)
	nestedDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	fileFetcher.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "9999999999999999999999999999999999999999999999999999999999999999", 512),
		buildDirectory,
		path.MustNewComponent("non-executable"),
		false).Return(nil)
	fileFetcher.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 512),
		buildDirectory,
		path.MustNewComponent("executable"),
		true).Return(nil)
	buildDirectory.EXPECT().Symlink("executable",
		path.MustNewComponent("link-to-executable")).Return(nil)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	require.NoError(t, err)
}

func TestNaiveBuildDirectoryInputRootNotInStorage(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil, status.Error(codes.Internal, "Storage is offline"))
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to obtain input directory \".\": Storage is offline"), err)
}

func TestNaiveBuildDirectoryMissingInputDirectoryDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "World",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to extract digest for input directory \"Hello/World\": No digest provided"), err)
}

func TestNaiveBuildDirectoryDirectoryCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "World",
				Digest: &remoteexecution.Digest{
					Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
					SizeBytes: 87,
				},
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	helloDirectory.EXPECT().Mkdir(path.MustNewComponent("World"), os.FileMode(0o777)).Return(status.Error(codes.DataLoss, "Disk on fire"))
	helloDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.DataLoss, "Failed to create input directory \"Hello/World\": Disk on fire"), err)
}

func TestNaiveBuildDirectoryDirectoryEnterDirectoryFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "World",
				Digest: &remoteexecution.Digest{
					Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
					SizeBytes: 87,
				},
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	helloDirectory.EXPECT().Mkdir(path.MustNewComponent("World"), os.FileMode(0o777)).Return(nil)
	helloDirectory.EXPECT().EnterDirectory(path.MustNewComponent("World")).Return(nil, status.Error(codes.PermissionDenied, "Thou shalt not pass!"))
	helloDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Failed to enter input directory \"Hello/World\": Thou shalt not pass!"), err)
}

func TestNaiveBuildDirectoryMissingInputFileDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "World",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to extract digest for input file \"Hello/World\": No digest provided"), err)
}

func TestNaiveBuildDirectoryFileCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "World",
				Digest: &remoteexecution.Digest{
					Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
					SizeBytes: 87,
				},
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	fileFetcher.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 87),
		helloDirectory,
		path.MustNewComponent("World"),
		false).Return(status.Error(codes.DataLoss, "Disk on fire"))
	helloDirectory.EXPECT().Close()
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.DataLoss, "Failed to obtain input file \"Hello/World\": Disk on fire"), err)
}

func TestNaiveBuildDirectorySymlinkCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "Hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8888888888888888888888888888888888888888888888888888888888888888",
					SizeBytes: 123,
				},
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "World",
				Target: "/etc/passwd",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("Hello"), os.FileMode(0o777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("Hello")).Return(helloDirectory, nil)
	helloDirectory.EXPECT().Symlink("/etc/passwd", path.MustNewComponent("World")).Return(status.Error(codes.Unimplemented, "This filesystem does not support symbolic links"))
	helloDirectory.EXPECT().Close()
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, directoryFetcher, fileFetcher, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", remoteexecution.DigestFunction_SHA256, "7777777777777777777777777777777777777777777777777777777777777777", 42),
		nil)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "Failed to create input symlink \"Hello/World\": This filesystem does not support symbolic links"), err)
}

func TestNaiveBuildDirectoryUploadFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	fileFetcher := mock.NewMockFileFetcher(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(
		buildDirectory,
		directoryFetcher,
		fileFetcher,
		contentAddressableStorage)

	helloWorldDigest := digest.MustNewDigest("default-scheduler", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)
	digestFunction := helloWorldDigest.GetDigestFunction()

	t.Run("NonexistentFile", func(t *testing.T) {
		buildDirectory.EXPECT().OpenRead(path.MustNewComponent("hello")).Return(nil, syscall.ENOENT)

		_, err := inputRootPopulator.UploadFile(ctx, path.MustNewComponent("hello"), digestFunction)
		require.Equal(t, syscall.ENOENT, err)
	})

	t.Run("IOFailureDuringDigestComputation", func(t *testing.T) {
		file := mock.NewMockFileReader(ctrl)
		buildDirectory.EXPECT().OpenRead(path.MustNewComponent("hello")).Return(file, nil)
		gomock.InOrder(
			file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
				func(p []byte, off int64) (int, error) {
					return 0, status.Error(codes.Unavailable, "Disk on fire")
				}),
			file.EXPECT().Close().Return(nil))

		_, err := inputRootPopulator.UploadFile(ctx, path.MustNewComponent("hello"), digestFunction)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to compute file digest: Disk on fire"), err)
	})

	t.Run("FileChangedDuringUpload", func(t *testing.T) {
		// Changes to the file contents between the digest
		// computation and upload phases should be detected.
		file := mock.NewMockFileReader(ctrl)
		buildDirectory.EXPECT().OpenRead(path.MustNewComponent("hello")).Return(file, nil)
		gomock.InOrder(
			file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
				func(p []byte, off int64) (int, error) {
					require.Greater(t, len(p), 11)
					copy(p, "Hello world")
					return 11, io.EOF
				}),
			file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
				func(p []byte, off int64) (int, error) {
					require.Greater(t, len(p), 9)
					copy(p, "Different")
					return 9, io.EOF
				}),
			file.EXPECT().Close().Return(nil))
		contentAddressableStorage.EXPECT().Put(ctx, helloWorldDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(100)
				testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Buffer is 9 bytes in size, while 11 bytes were expected"), err)
				return err
			})

		_, err := inputRootPopulator.UploadFile(ctx, path.MustNewComponent("hello"), digestFunction)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to upload file: Buffer is 9 bytes in size, while 11 bytes were expected"), err)
	})

	t.Run("SuccessFileGrownDuringUpload", func(t *testing.T) {
		// Simulate the case where the file to be uploaded grows
		// while being uploaded. The newly added part should be
		// ignored, as it wasn't used to compute the digest.
		// This is not uncommon, especially for stdout and
		// stderr logs.
		file := mock.NewMockFileReader(ctrl)
		buildDirectory.EXPECT().OpenRead(path.MustNewComponent("hello")).Return(file, nil)
		gomock.InOrder(
			file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
				func(p []byte, off int64) (int, error) {
					require.Greater(t, len(p), 11)
					copy(p, "Hello world")
					return 11, io.EOF
				}),
			file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
				func(p []byte, off int64) (int, error) {
					require.Len(t, p, 11)
					copy(p, "Hello world")
					return 11, nil
				}),
			file.EXPECT().Close().Return(nil))
		contentAddressableStorage.EXPECT().Put(ctx, helloWorldDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		digest, err := inputRootPopulator.UploadFile(ctx, path.MustNewComponent("hello"), digestFunction)
		require.NoError(t, err)
		require.Equal(t, digest, helloWorldDigest)
	})
}
