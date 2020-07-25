package builder_test

import (
	"context"
	"os"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNaiveBuildDirectorySuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
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
	buildDirectory.EXPECT().Mkdir("directory", os.FileMode(0777)).Return(nil)
	nestedDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("directory").Return(nestedDirectory, nil)
	nestedDirectory.EXPECT().Symlink("../non-executable", "link-to-non-executable").Return(nil)
	nestedDirectory.EXPECT().Close()
	contentAddressableStorage.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", "9999999999999999999999999999999999999999999999999999999999999999", 512),
		buildDirectory,
		"non-executable",
		false).Return(nil)
	contentAddressableStorage.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 512),
		buildDirectory,
		"executable",
		true).Return(nil)
	buildDirectory.EXPECT().Symlink("executable", "link-to-executable").Return(nil)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.NoError(t, err)
}

func TestNaiveBuildDirectoryInputRootNotInStorage(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil, status.Error(codes.Internal, "Storage is offline"))
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.Internal, "Failed to obtain input directory \".\": Storage is offline"))
}

func TestNaiveBuildDirectoryMissingInputDirectoryDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "World",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.InvalidArgument, "Failed to extract digest for input directory \"Hello/World\": No digest provided"))
}

func TestNaiveBuildDirectoryDirectoryCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
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
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Mkdir("World", os.FileMode(0777)).Return(status.Error(codes.DataLoss, "Disk on fire"))
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.DataLoss, "Failed to create input directory \"Hello/World\": Disk on fire"))
}

func TestNaiveBuildDirectoryDirectoryEnterDirectoryFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
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
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Mkdir("World", os.FileMode(0777)).Return(nil)
	helloDirectory.EXPECT().EnterDirectory("World").Return(nil, status.Error(codes.PermissionDenied, "Thou shalt not pass!"))
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.PermissionDenied, "Failed to enter input directory \"Hello/World\": Thou shalt not pass!"))
}

func TestNaiveBuildDirectoryMissingInputFileDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
	).Return(&remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "World",
			},
		},
	}, nil)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	buildDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.InvalidArgument, "Failed to extract digest for input file \"Hello/World\": No digest provided"))
}

func TestNaiveBuildDirectoryFileCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
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
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	contentAddressableStorage.EXPECT().GetFile(
		ctx,
		digest.MustNewDigest("netbsd", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 87),
		helloDirectory,
		"World",
		false).Return(status.Error(codes.DataLoss, "Disk on fire"))
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.DataLoss, "Failed to obtain input file \"Hello/World\": Disk on fire"))
}

func TestNaiveBuildDirectorySymlinkCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
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
	contentAddressableStorage.EXPECT().GetDirectory(
		ctx,
		digest.MustNewDigest("netbsd", "8888888888888888888888888888888888888888888888888888888888888888", 123),
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
	buildDirectory.EXPECT().Mkdir("Hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	buildDirectory.EXPECT().EnterDirectory("Hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Symlink("/etc/passwd", "World").Return(status.Error(codes.Unimplemented, "This filesystem does not support symbolic links"))
	helloDirectory.EXPECT().Close()
	inputRootPopulator := builder.NewNaiveBuildDirectory(buildDirectory, contentAddressableStorage)

	err := inputRootPopulator.MergeDirectoryContents(
		ctx,
		errorLogger,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42))
	require.Equal(t, err, status.Error(codes.Unimplemented, "Failed to create input symlink \"Hello/World\": This filesystem does not support symbolic links"))
}
