//go:build linux
// +build linux

package runner_test

import (
	"context"
	"fmt"
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

func TestCgroupResourceUsageSamplingRunnerReportsDeltasFromCgroupFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			writeFile(t, cgroupPath, "memory.events", `
low 11
high 22
max 33
oom 44
oom_kill 55
oom_group_kill 66
`)
			writeFile(t, cgroupPath, "memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=160
full avg10=0.00 avg60=0.00 avg300=0.00 total=290
`)
			writeFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=345
full avg10=0.00 avg60=0.00 avg300=0.00 total=410
`)
			writeFile(t, cgroupPath, "io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=480
full avg10=0.00 avg60=0.00 avg300=0.00 total=610
`)
			writeFile(t, cgroupPath, "memory.peak", "4096\n")
			return &runner_pb.RunResponse{
				ExitCode: 7,
			}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(7), response.ExitCode)
	require.Len(t, response.ResourceUsage, 1)
	var usage resourceusage.CgroupResourceUsage
	require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&usage))

	require.Equal(t, int64(10), usage.MemoryEventsLow)
	require.Equal(t, int64(20), usage.MemoryEventsHigh)
	require.Equal(t, int64(30), usage.MemoryEventsMax)
	require.Equal(t, int64(40), usage.MemoryEventsOom)
	require.Equal(t, int64(50), usage.MemoryEventsOomKill)
	require.Equal(t, int64(60), usage.MemoryEventsOomGroupKill)
	require.Equal(t, int64(4096), usage.MemoryPeak)
	require.Equal(t, 60*time.Microsecond, usage.GetMemoryPressureSomeTotal().AsDuration())
	require.Equal(t, 90*time.Microsecond, usage.GetMemoryPressureFullTotal().AsDuration())
	require.Equal(t, 45*time.Microsecond, usage.GetCpuPressureSomeTotal().AsDuration())
	require.Equal(t, 100*time.Microsecond, usage.GetCpuPressureFullTotal().AsDuration())
	require.Equal(t, 80*time.Microsecond, usage.GetIoPressureSomeTotal().AsDuration())
	require.Equal(t, 110*time.Microsecond, usage.GetIoPressureFullTotal().AsDuration())
}

func TestCgroupResourceUsageSamplingRunnerAllowsMissingOomGroupKillCounter(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	writeFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 5
`)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			writeFile(t, cgroupPath, "memory.events", `
low 11
high 22
max 33
oom 44
oom_kill 55
`)
			return &runner_pb.RunResponse{}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.NoError(t, err)
	require.Len(t, response.ResourceUsage, 1)
	var usage resourceusage.CgroupResourceUsage
	require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&usage))

	require.Equal(t, int64(50), usage.MemoryEventsOomKill)
	require.Equal(t, int64(0), usage.MemoryEventsOomGroupKill)
}

func TestResolveCurrentCgroupfsPathUsesMatchingCgroup2MountRoot(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		mountInfo string
		cgroup    string
		wantPath  string
	}{
		{
			name: "root mount",
			mountInfo: `
36 25 0:31 / %[1]s rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup:   "0::/worker.slice/runner.scope\n",
			wantPath: "worker.slice/runner.scope",
		},
		{
			name: "subtree mount",
			mountInfo: `
36 25 0:31 /unrelated /wrong rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
37 25 0:31 /worker.slice %[1]s rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup:   "0::/worker.slice/runner.scope\n",
			wantPath: "runner.scope",
		},
		{
			name: "most specific mount root",
			mountInfo: `
36 25 0:31 / %[1]s/unrelated rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
37 25 0:31 /worker.slice %[1]s rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup:   "0::/worker.slice/runner.scope\n",
			wantPath: "runner.scope",
		},
		{
			name: "current cgroup is mount root",
			mountInfo: `
36 25 0:31 /worker.slice %[1]s rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup:   "0::/worker.slice\n",
			wantPath: ".",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tempDir := t.TempDir()
			cgroupfsPath := filepath.Join(tempDir, "cgroupfs")
			resolvedCgroupPath := filepath.Join(cgroupfsPath, testCase.wantPath)

			mountInfoPath := filepath.Join(tempDir, "mountinfo")
			procCgroupPath := filepath.Join(tempDir, "cgroup")
			writeFile(t, tempDir, "mountinfo", fmt.Sprintf(testCase.mountInfo, cgroupfsPath))
			writeFile(t, tempDir, "cgroup", testCase.cgroup)

			gotCgroupfsPath, err := runner.ResolveCurrentCgroupfsPathFromProcFiles(procCgroupPath, mountInfoPath)
			require.NoError(t, err)
			require.Equal(t, resolvedCgroupPath, gotCgroupfsPath)
		})
	}
}

func TestCgroupResourceUsageSamplingRunnerAppendsResourceUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	request := &runner_pb.RunRequest{}
	baseRunner.EXPECT().Run(gomock.Any(), request).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			writeFile(t, cgroupPath, "memory.events", `
low 1
high 3
max 3
oom 4
oom_kill 5
oom_group_kill 6
`)
			writeFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=423
full avg10=0.00 avg60=0.00 avg300=0.00 total=355
`)
			writeFile(t, cgroupPath, "memory.peak", "4096\n")
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

func TestCgroupResourceUsageSamplingRunnerResetsMemoryPeakBeforeRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			memoryPeak, err := os.ReadFile(filepath.Join(cgroupPath, "memory.peak"))
			require.NoError(t, err)
			require.Equal(t, "1\n", string(memoryPeak))
			writeFile(t, cgroupPath, "memory.peak", "4096\n")
			return &runner_pb.RunResponse{}, nil
		},
	)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.NoError(t, err)
	require.Len(t, response.ResourceUsage, 1)
	var got resourceusage.CgroupResourceUsage
	require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&got))
	require.Equal(t, int64(4096), got.GetMemoryPeak())
}

func TestCgroupResourceUsageSamplingRunnerReadErrorIsPropagated(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupPath := createTestCgroup(t)
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

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
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

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
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			writeFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 6
oom_group_kill 6
`)
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
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			writeFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 5
oom_kill 6
oom_group_kill 6
`)
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
	wrappedRunner := runner.NewCgroupResourceUsageSamplingRunnerWithCgroupfsPath(baseRunner, cgroupPath)

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

func createTestCgroup(t *testing.T) string {
	cgroupPath := t.TempDir()
	writeFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 5
oom_group_kill 6
`)
	writeFile(t, cgroupPath, "memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=100
full avg10=0.00 avg60=0.00 avg300=0.00 total=200
`)
	writeFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=300
full avg10=0.00 avg60=0.00 avg300=0.00 total=310
`)
	writeFile(t, cgroupPath, "io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=400
full avg10=0.00 avg60=0.00 avg300=0.00 total=500
`)
	writeFile(t, cgroupPath, "memory.peak", "0\n")
	return cgroupPath
}

func writeFile(t *testing.T, dir, name, contents string) {
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o666))
}
