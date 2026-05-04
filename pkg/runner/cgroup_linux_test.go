//go:build linux

package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCgroupStatsReaderReportsDeltasFromCgroupFiles(t *testing.T) {
	cgroupPath := t.TempDir()

	writeCgroupFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 5
oom_group_kill 6
`)
	writeCgroupFile(t, cgroupPath, "memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=100
full avg10=0.00 avg60=0.00 avg300=0.00 total=200
`)
	writeCgroupFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=300
full avg10=0.00 avg60=0.00 avg300=0.00 total=310
`)
	writeCgroupFile(t, cgroupPath, "io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=400
full avg10=0.00 avg60=0.00 avg300=0.00 total=500
`)
	writeCgroupFile(t, cgroupPath, "memory.peak", "0\n")

	reader, err := newCgroupStatsReader(cgroupPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	writeCgroupFile(t, cgroupPath, "memory.events", `
low 11
high 22
max 33
oom 44
oom_kill 55
oom_group_kill 66
`)
	writeCgroupFile(t, cgroupPath, "memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=160
full avg10=0.00 avg60=0.00 avg300=0.00 total=290
`)
	writeCgroupFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=345
full avg10=0.00 avg60=0.00 avg300=0.00 total=410
`)
	writeCgroupFile(t, cgroupPath, "io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=480
full avg10=0.00 avg60=0.00 avg300=0.00 total=610
`)
	writeCgroupFile(t, cgroupPath, "memory.peak", "4096\n")

	usage, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, int64(10), usage.MemoryEventsLow)
	require.Equal(t, int64(20), usage.MemoryEventsHigh)
	require.Equal(t, int64(30), usage.MemoryEventsMax)
	require.Equal(t, int64(40), usage.MemoryEventsOom)
	require.Equal(t, int64(50), usage.MemoryEventsOomKill)
	require.Equal(t, int64(60), usage.MemoryEventsOomGroupKill)
	require.Equal(t, int64(4096), usage.MemoryPeakBytes)
	require.Equal(t, 60*time.Microsecond, usage.GetPsiMemorySome().AsDuration())
	require.Equal(t, 90*time.Microsecond, usage.GetPsiMemoryFull().AsDuration())
	require.Equal(t, 45*time.Microsecond, usage.GetPsiCpuSome().AsDuration())
	require.Equal(t, 100*time.Microsecond, usage.GetPsiCpuFull().AsDuration())
	require.Equal(t, 80*time.Microsecond, usage.GetPsiIoSome().AsDuration())
	require.Equal(t, 110*time.Microsecond, usage.GetPsiIoFull().AsDuration())
}

func TestCgroupStatsReaderAllowsMissingOomGroupKillCounter(t *testing.T) {
	cgroupPath := t.TempDir()

	writeCgroupFile(t, cgroupPath, "memory.events", `
low 1
high 2
max 3
oom 4
oom_kill 5
`)
	writeCgroupFile(t, cgroupPath, "memory.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=100
full avg10=0.00 avg60=0.00 avg300=0.00 total=200
`)
	writeCgroupFile(t, cgroupPath, "cpu.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=300
full avg10=0.00 avg60=0.00 avg300=0.00 total=310
`)
	writeCgroupFile(t, cgroupPath, "io.pressure", `
some avg10=0.00 avg60=0.00 avg300=0.00 total=400
full avg10=0.00 avg60=0.00 avg300=0.00 total=500
`)
	writeCgroupFile(t, cgroupPath, "memory.peak", "0\n")

	reader, err := newCgroupStatsReader(cgroupPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	writeCgroupFile(t, cgroupPath, "memory.events", `
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

func TestResolveCurrentCgroupPathUsesMatchingCgroup2MountRoot(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		mountInfo string
		cgroup    string
		want      string
	}{
		{
			name: "root mount",
			mountInfo: `
36 25 0:31 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup: "0::/worker.slice/runner.scope\n",
			want:   "/sys/fs/cgroup/worker.slice/runner.scope",
		},
		{
			name: "subtree mount",
			mountInfo: `
36 25 0:31 /unrelated /wrong rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
37 25 0:31 /worker.slice /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup: "0::/worker.slice/runner.scope\n",
			want:   "/sys/fs/cgroup/runner.scope",
		},
		{
			name: "most specific mount root",
			mountInfo: `
36 25 0:31 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
37 25 0:31 /worker.slice /run/worker-cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup: "0::/worker.slice/runner.scope\n",
			want:   "/run/worker-cgroup/runner.scope",
		},
		{
			name: "current cgroup is mount root",
			mountInfo: `
36 25 0:31 /worker.slice /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
`,
			cgroup: "0::/worker.slice\n",
			want:   "/sys/fs/cgroup",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tempDir := t.TempDir()
			mountInfoPath := filepath.Join(tempDir, "mountinfo")
			cgroupPath := filepath.Join(tempDir, "cgroup")
			writeFile(t, mountInfoPath, testCase.mountInfo)
			writeFile(t, cgroupPath, testCase.cgroup)

			got, err := resolveCurrentCgroupPath(mountInfoPath, cgroupPath)
			require.NoError(t, err)
			require.Equal(t, testCase.want, got)
		})
	}
}

func writeCgroupFile(t *testing.T, cgroupPath, name, contents string) {
	writeFile(t, filepath.Join(cgroupPath, name), contents)
}

func writeFile(t *testing.T, path, contents string) {
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o666))
}
