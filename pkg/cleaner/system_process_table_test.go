package cleaner_test

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/stretchr/testify/require"
)

func TestSystemProcessTable(t *testing.T) {
	// TODO: Implement this functionality on non-Linux platforms.
	if runtime.GOOS == "freebsd" || runtime.GOOS == "windows" {
		return
	}

	processes, err := cleaner.SystemProcessTable.GetProcesses()
	require.NoError(t, err)

	// The returned process table should contain the currently
	// running process. The user ID and creation time should also be
	// sensible.
	// TODO: Doesn't testify provide a require.Contains() that takes
	// a custom matcher function?
	found := false
	processID := os.Getpid()
	for _, process := range processes {
		if process.ProcessID == processID {
			found = true
			require.Equal(t, os.Getuid(), process.UserID)
			require.True(t, process.CreationTime.After(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))
			require.False(t, process.CreationTime.After(time.Now()))
			break
		}
	}
	require.True(t, found)
}

func TestSystemProcessTableCreationTime(t *testing.T) {
	if runtime.GOOS != "linux" {
		return
	}

	// The creation time of a process should not depend on when its
	// entry in the process table is first read. On Linux, the
	// timestamps of procfs entries are set when first looked up.
	child := exec.Command("sleep", "10")
	require.NoError(t, child.Start())
	defer child.Process.Kill()
	started := time.Now()
	time.Sleep(100 * time.Millisecond)

	processes, err := cleaner.SystemProcessTable.GetProcesses()
	require.NoError(t, err)
	found := false
	for _, process := range processes {
		if process.ProcessID == child.Process.Pid {
			found = true
			require.False(t, process.CreationTime.After(started))
			require.True(t, process.CreationTime.After(started.Add(-time.Second)))
			break
		}
	}
	require.True(t, found)
}
