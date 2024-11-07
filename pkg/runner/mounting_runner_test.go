package runner_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMountingRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	mountpoint := "proc_inside"
	mountpointComponent := path.MustNewComponent(mountpoint)
	source := "/proc"
	fstype := "proc"

	runRequest := &runner_pb.RunRequest{
		Arguments: []string{"ls", "-l", mountpoint},
	}

	t.Run("Run", func(t *testing.T) {
		mockRunner := mock.NewMockRunnerServer(ctrl)
		mockDirectory := mock.NewMockDirectory(ctrl)

		runnerServer := runner.NewMountingRunner(mockRunner, mockDirectory, &bb_runner.InputMountOptions{
			Mountpoint:     mountpoint,
			Source:         source,
			FilesystemType: fstype,
		})
		mockDirectory.EXPECT().Mount(mountpointComponent, source, fstype)
		mockDirectory.EXPECT().Unmount(mountpointComponent)

		mockRunner.EXPECT().Run(ctx, runRequest).Return(&runner_pb.RunResponse{}, nil)
		_, err := runnerServer.Run(ctx, runRequest)
		require.NoError(t, err)
	})

	t.Run("RunChained", func(t *testing.T) {
		mockRunner := mock.NewMockRunnerServer(ctrl)
		mockDirectory := mock.NewMockDirectory(ctrl)

		innerDecorator := runner.NewMountingRunner(mockRunner, mockDirectory, &bb_runner.InputMountOptions{
			Mountpoint:     mountpoint,
			Source:         source,
			FilesystemType: fstype,
		})
		outer := []string{"sys_inside", "/sys", "sysfs"}
		outerDecorator := runner.NewMountingRunner(innerDecorator, mockDirectory, &bb_runner.InputMountOptions{
			Mountpoint:     outer[0],
			Source:         outer[1],
			FilesystemType: outer[2],
		})

		gomock.InOrder(
			mockDirectory.EXPECT().Mount(path.MustNewComponent(outer[0]), outer[1], outer[2]),
			mockDirectory.EXPECT().Mount(mountpointComponent, source, fstype),
			mockDirectory.EXPECT().Unmount(mountpointComponent),
			mockDirectory.EXPECT().Unmount(path.MustNewComponent(outer[0])),
		)

		mockRunner.EXPECT().Run(ctx, runRequest).Return(&runner_pb.RunResponse{}, nil)
		_, err := outerDecorator.Run(ctx, runRequest)
		require.NoError(t, err)
	})

	t.Run("RunNestedMount", func(t *testing.T) {
		mockRunner := mock.NewMockRunnerServer(ctrl)
		mockDirectory := mock.NewMockDirectory(ctrl)
		mockDirectoryCloser := mock.NewMockDirectoryCloser(ctrl)

		nestedMountpoint := path.MustNewComponent("nested")
		parent := path.MustNewComponent("outer")
		fullPath := filepath.Join(parent.String(), nestedMountpoint.String())
		runnerServer := runner.NewMountingRunner(mockRunner, mockDirectory, &bb_runner.InputMountOptions{
			Mountpoint:     fullPath,
			Source:         source,
			FilesystemType: fstype,
		})

		// The mount calls are called in a subdirectory
		mockDirectory.EXPECT().EnterDirectory(parent).Return(mockDirectoryCloser, nil)
		mockDirectoryCloser.EXPECT().Mount(nestedMountpoint, source, fstype)
		mockDirectoryCloser.EXPECT().Unmount(nestedMountpoint)
		mockDirectoryCloser.EXPECT().Close().Times(2) // Why twice ???

		mockRunner.EXPECT().Run(ctx, runRequest).Return(&runner_pb.RunResponse{}, nil)
		_, err := runnerServer.Run(ctx, runRequest)
		require.NoError(t, err)
	})
}
