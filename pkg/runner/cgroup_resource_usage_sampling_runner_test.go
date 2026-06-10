//go:build linux

package runner_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCgroupResourceUsageSamplingRunnerAppendsResourceUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	request := &runner_pb.RunRequest{}
	baseRunner.EXPECT().Run(gomock.Any(), request).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "memory.events"), []byte(`
low 0
high 1
max 0
oom 0
oom_kill 0
oom_group_kill 0
`), 0o666))
			require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "cpu.pressure"), []byte(`
some avg10=0.00 avg60=0.00 avg300=0.00 total=123
full avg10=0.00 avg60=0.00 avg300=0.00 total=45
`), 0o666))
			require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "memory.peak"), []byte("4096\n"), 0o666))
			return &runner_pb.RunResponse{
				ExitCode: 7,
			}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, int64(7), response.ExitCode)
	require.Len(t, response.ResourceUsage, 1)
	var got resourceusage.CgroupResourceUsage
	require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&got))
	require.Equal(t, int64(1), got.GetMemoryEventsHigh())
	require.Equal(t, int64(4096), got.GetMemoryPeak())
	require.Equal(t, 123*time.Microsecond, got.GetCpuPressureSomeTotal().AsDuration())
	require.Equal(t, 45*time.Microsecond, got.GetCpuPressureFullTotal().AsDuration())
}

func TestCgroupResourceUsageSamplingRunnerReadErrorIsPropagated(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	request := &runner_pb.RunRequest{}
	baseResponse := &runner_pb.RunResponse{ExitCode: 7}
	baseRunner.EXPECT().Run(gomock.Any(), request).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			require.NoError(t, os.Remove(filepath.Join(cgroupPath, "memory.events")))
			return baseResponse, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), request)
	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "Failed to read cgroup stats")
	require.Same(t, baseResponse, response)
	require.Empty(t, response.ResourceUsage)
}

func TestCgroupResourceUsageSamplingRunnerRunErrorDoesNotReadCgroupUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	baseErr := status.Error(codes.FailedPrecondition, "failed")
	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			require.NoError(t, os.Remove(filepath.Join(cgroupPath, "memory.events")))
			return nil, baseErr
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.Nil(t, response)
	require.Equal(t, baseErr, err)
}

func TestCgroupResourceUsageSamplingRunnerSystemOOMKillIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "memory.events"), []byte(`
low 0
high 0
max 0
oom 0
oom_kill 1
oom_group_kill 0
`), 0o666))
			return &runner_pb.RunResponse{
				ExitCode: 0,
			}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "An action process was OOM-killed without the action reaching its cgroup memory limit", status.Convert(err).Message())
	require.NotNil(t, response)
	require.Len(t, response.ResourceUsage, 1)
}

func TestCgroupResourceUsageSamplingRunnerCgroupOOMKillIsActionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "memory.events"), []byte(`
low 0
high 0
max 0
oom 1
oom_kill 1
oom_group_kill 0
`), 0o666))
			return &runner_pb.RunResponse{
				ExitCode: 0,
			}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(0), response.ExitCode)
	require.Len(t, response.ResourceUsage, 1)
}

func TestCgroupResourceUsageSamplingRunnerRejectsConcurrentRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := newTestCgroupResourceUsageSamplingRunner(baseRunner, cgroupPath)

	started := make(chan struct{})
	release := make(chan struct{})
	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			close(started)
			<-release
			return &runner_pb.RunResponse{}, nil
		},
	)

	firstRunErr := make(chan error, 1)
	go func() {
		_, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
		firstRunErr <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Run() to enter wrapped runner")
	}

	secondRunDone := make(chan struct{})
	var response *runner_pb.RunResponse
	var err error
	go func() {
		response, err = wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
		close(secondRunDone)
	}()
	select {
	case <-secondRunDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for concurrent Run() rejection")
	}
	require.Nil(t, response)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "concurrent Run() calls")

	close(release)
	select {
	case err := <-firstRunErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Run() to finish")
	}
}

func newTestCgroupResourceUsageSamplingRunner(base runner_pb.RunnerServer, cgroupPath string) runner_pb.RunnerServer {
	return runner.NewCgroupResourceUsageSamplingRunner(base, cgroupPath)
}

func createTestCgroup(t *testing.T) string {
	cgroupPath := t.TempDir()
	writeFile := func(name, contents string) {
		require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, name), []byte(contents), 0o666))
	}
	writeFile("memory.events", `
low 0
high 0
max 0
oom 0
oom_kill 0
oom_group_kill 0
`)
	writeFile("memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=0
full avg10=0.00 avg60=0.00 avg300=0.00 total=0
`)
	writeFile("cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=0
full avg10=0.00 avg60=0.00 avg300=0.00 total=0
`)
	writeFile("io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=0
full avg10=0.00 avg60=0.00 avg300=0.00 total=0
`)
	writeFile("memory.peak", "0\n")
	return cgroupPath
}
