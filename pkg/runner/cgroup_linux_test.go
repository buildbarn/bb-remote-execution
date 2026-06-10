//go:build linux

package runner_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/stretchr/testify/require"
)

func TestCgroupResourceUsageReaderReportsDeltasFromCgroupFiles(t *testing.T) {
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

	reader, err := runner.NewCgroupResourceUsageReaderFromPath(cgroupPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

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

	usage, err := reader.Read()
	require.NoError(t, err)

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

func TestCgroupResourceUsageReaderAllowsMissingOomGroupKillCounter(t *testing.T) {
	cgroupPath := t.TempDir()

	writeFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 5
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

	reader, err := runner.NewCgroupResourceUsageReaderFromPath(cgroupPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	writeFile(t, cgroupPath, "memory.events", `
low 11
high 22
max 33
oom 44
oom_kill 55
`)

	usage, err := reader.Read()
	require.NoError(t, err)

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
			require.NoError(t, os.MkdirAll(resolvedCgroupPath, 0o777))
			writeInitialCgroupResourceUsageFiles(t, resolvedCgroupPath)

			mountInfoPath := filepath.Join(tempDir, "mountinfo")
			procCgroupPath := filepath.Join(tempDir, "cgroup")
			writeFile(t, tempDir, "mountinfo", fmt.Sprintf(testCase.mountInfo, cgroupfsPath))
			writeFile(t, tempDir, "cgroup", testCase.cgroup)

			gotCgroupfsPath, err := runner.ResolveCurrentCgroupfsPathFromProcFiles(procCgroupPath, mountInfoPath)
			require.NoError(t, err)
			require.Equal(t, resolvedCgroupPath, gotCgroupfsPath)

			reader, err := runner.NewCgroupResourceUsageReaderFromPath(gotCgroupfsPath)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, reader.Close())
			}()

			writeFile(t, resolvedCgroupPath, "memory.events", `
low 11
high 22
max 33
oom 44
oom_kill 55
oom_group_kill 66
`)
			usage, err := reader.Read()
			require.NoError(t, err)
			require.Equal(t, int64(10), usage.MemoryEventsLow)
		})
	}
}

func writeInitialCgroupResourceUsageFiles(t *testing.T, cgroupPath string) {
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
}

func writeFile(t *testing.T, dir, name, contents string) {
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o666))
}
