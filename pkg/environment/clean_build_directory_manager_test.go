package environment_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCleanBuildDirectoryManagerAcquireFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Failure to create environment should simply be forwarded.
	baseManager := mock.NewMockManager(ctrl)
	baseManager.EXPECT().Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		}).Return(nil, status.Error(codes.Internal, "No space left on device"))

	manager := environment.NewCleanBuildDirectoryManager(baseManager)
	_, err := manager.Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		})
	require.Equal(t, status.Error(codes.Internal, "No space left on device"), err)
}

func TestCleanBuildDirectoryManagerRemoveAllChildrenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Failure to clean the build subdirectory is always an internal error.
	baseManager := mock.NewMockManager(ctrl)
	baseEnvironment := mock.NewMockManagedEnvironment(ctrl)
	baseManager.EXPECT().Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		}).Return(baseEnvironment, nil)
	rootDirectory := mock.NewMockDirectory(ctrl)
	baseEnvironment.EXPECT().GetBuildDirectory().Return(rootDirectory).AnyTimes()
	rootDirectory.EXPECT().RemoveAllChildren().Return(
		status.Error(codes.PermissionDenied, "You don't have permissions to remove files from disk"))
	baseEnvironment.EXPECT().Release()

	manager := environment.NewCleanBuildDirectoryManager(baseManager)
	_, err := manager.Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		})
	require.Equal(t, status.Error(codes.Internal, "Failed to clean build directory prior to build: You don't have permissions to remove files from disk"), err)
}

func TestCleanBuildDirectoryManagerSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Successful build in a clean build directory.
	baseManager := mock.NewMockManager(ctrl)
	baseEnvironment := mock.NewMockManagedEnvironment(ctrl)
	baseManager.EXPECT().Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		}).Return(baseEnvironment, nil)
	rootDirectory := mock.NewMockDirectory(ctrl)
	baseEnvironment.EXPECT().GetBuildDirectory().Return(rootDirectory).AnyTimes()
	rootDirectory.EXPECT().RemoveAllChildren().Return(nil)
	baseEnvironment.EXPECT().Run(ctx, &runner.RunRequest{
		Arguments: []string{"ls", "-l"},
		EnvironmentVariables: map[string]string{
			"PATH": "/bin",
		},
		WorkingDirectory: "some/sub/directory",
		StdoutPath:       ".stdout.txt",
		StderrPath:       ".stderr.txt",
	}).Return(&runner.RunResponse{
		ExitCode: 123,
	}, nil)
	baseEnvironment.EXPECT().Release()

	manager := environment.NewCleanBuildDirectoryManager(baseManager)
	environment, err := manager.Acquire(
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}),
		map[string]string{
			"container-image": "ubuntu:latest",
		})
	require.NoError(t, err)
	require.Equal(t, rootDirectory, environment.GetBuildDirectory())
	response, err := environment.Run(ctx, &runner.RunRequest{
		Arguments: []string{"ls", "-l"},
		EnvironmentVariables: map[string]string{
			"PATH": "/bin",
		},
		WorkingDirectory: "some/sub/directory",
		StdoutPath:       ".stdout.txt",
		StderrPath:       ".stderr.txt",
	})
	require.NoError(t, err)
	require.Equal(t, &runner.RunResponse{
		ExitCode: 123,
	}, response)
	environment.Release()
}
