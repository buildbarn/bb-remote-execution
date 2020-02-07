package environment_test

import (
	"context"
	"os"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActionDigestSubdirectoryManagerAcquireFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Failure to create environment should simply be forwarded.
	baseManager := mock.NewMockManager(ctrl)
	baseManager.EXPECT().Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
	).Return(nil, status.Error(codes.Internal, "No space left on device"))

	manager := environment.NewActionDigestSubdirectoryManager(baseManager)
	_, err := manager.Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0))
	require.Equal(t, status.Error(codes.Internal, "No space left on device"), err)
}

func TestActionDigestSubdirectoryManagerMkdirFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Failure to create a build subdirectory is always an internal error.
	baseManager := mock.NewMockManager(ctrl)
	baseEnvironment := mock.NewMockManagedEnvironment(ctrl)
	baseManager.EXPECT().Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
	).Return(baseEnvironment, nil)
	rootDirectory := mock.NewMockDirectory(ctrl)
	baseEnvironment.EXPECT().GetBuildDirectory().Return(rootDirectory).AnyTimes()
	rootDirectory.EXPECT().Mkdir("e3b0c44298fc1c14", os.FileMode(0777)).Return(
		status.Error(codes.AlreadyExists, "Directory already exists"))
	baseEnvironment.EXPECT().Release()

	manager := environment.NewActionDigestSubdirectoryManager(baseManager)
	_, err := manager.Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0))
	require.Equal(t, status.Error(codes.Internal, "Failed to create build subdirectory \"e3b0c44298fc1c14\": Directory already exists"), err)
}

func TestActionDigestSubdirectoryManagerEnterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Failure to enter a build subdirectory is always an internal error.
	baseManager := mock.NewMockManager(ctrl)
	baseEnvironment := mock.NewMockManagedEnvironment(ctrl)
	baseManager.EXPECT().Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
	).Return(baseEnvironment, nil)
	rootDirectory := mock.NewMockDirectory(ctrl)
	baseEnvironment.EXPECT().GetBuildDirectory().Return(rootDirectory).AnyTimes()
	rootDirectory.EXPECT().Mkdir("e3b0c44298fc1c14", os.FileMode(0777)).Return(nil)
	rootDirectory.EXPECT().Enter("e3b0c44298fc1c14").Return(nil, status.Error(codes.ResourceExhausted, "Out of file descriptors"))
	rootDirectory.EXPECT().Remove("e3b0c44298fc1c14").Return(nil)
	baseEnvironment.EXPECT().Release()

	manager := environment.NewActionDigestSubdirectoryManager(baseManager)
	_, err := manager.Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0))
	require.Equal(t, status.Error(codes.Internal, "Failed to enter build subdirectory \"e3b0c44298fc1c14\": Out of file descriptors"), err)
}

func TestActionDigestSubdirectoryManagerSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Successful build in a subdirectory.
	baseManager := mock.NewMockManager(ctrl)
	baseEnvironment := mock.NewMockManagedEnvironment(ctrl)
	baseManager.EXPECT().Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0),
	).Return(baseEnvironment, nil)
	rootDirectory := mock.NewMockDirectory(ctrl)
	baseEnvironment.EXPECT().GetBuildDirectory().Return(rootDirectory).AnyTimes()
	rootDirectory.EXPECT().Mkdir("e3b0c44298fc1c14", os.FileMode(0777)).Return(nil)
	subDirectory := mock.NewMockDirectory(ctrl)
	rootDirectory.EXPECT().Enter("e3b0c44298fc1c14").Return(subDirectory, nil)
	baseEnvironment.EXPECT().Run(ctx, &runner.RunRequest{
		Arguments: []string{"ls", "-l"},
		EnvironmentVariables: map[string]string{
			"PATH": "/bin",
		},
		WorkingDirectory: "e3b0c44298fc1c14/some/sub/directory",
		StdoutPath:       "e3b0c44298fc1c14/.stdout.txt",
		StderrPath:       "e3b0c44298fc1c14/.stderr.txt",
	}).Return(&runner.RunResponse{
		ExitCode: 123,
	}, nil)
	subDirectory.EXPECT().Close()
	rootDirectory.EXPECT().RemoveAll("e3b0c44298fc1c14").Return(nil)
	baseEnvironment.EXPECT().Release()

	manager := environment.NewActionDigestSubdirectoryManager(baseManager)
	environment, err := manager.Acquire(
		digest.MustNewDigest("debian8", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0))
	require.NoError(t, err)
	require.Equal(t, subDirectory, environment.GetBuildDirectory())
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
