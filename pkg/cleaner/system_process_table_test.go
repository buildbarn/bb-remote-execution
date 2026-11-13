package cleaner_test

import (
	"os"
	"os/exec"
	"path/filepath"
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

// getProcessCreationTime returns the creation time that the system
// process table reports for a given process.
func getProcessCreationTime(t *testing.T, processID int) time.Time {
	processes, err := cleaner.SystemProcessTable.GetProcesses()
	require.NoError(t, err)
	for _, process := range processes {
		if process.ProcessID == processID {
			return process.CreationTime
		}
	}
	require.FailNowf(t, "Process not found", "Process %d is not part of the process table", processID)
	return time.Time{}
}

func TestSystemProcessTableCreationTime(t *testing.T) {
	// TODO: Implement this functionality on non-Linux platforms.
	if runtime.GOOS == "freebsd" || runtime.GOOS == "windows" {
		return
	}
	testStart := time.Now()

	// Creation times are derived from clocks that have a limited
	// resolution (e.g., clock ticks on Linux). Permit them to lie
	// this much before the actual creation of the process.
	const slack = time.Second

	// startProcess launches a process that remains idle until the
	// test completes, and returns the time interval in which it
	// got created. The first argument is always set to "sleep", so
	// that multi-call binaries (e.g., BusyBox) behave as such when
	// launched through a path with a different name.
	startProcess := func(t *testing.T, path string) (int, time.Time, time.Time) {
		cmd := exec.Command(path, "3600")
		cmd.Args[0] = "sleep"
		beforeStart := time.Now()
		require.NoError(t, cmd.Start())
		afterStart := time.Now()
		t.Cleanup(func() {
			cmd.Process.Kill()
			cmd.Wait()
		})
		return cmd.Process.Pid, beforeStart, afterStart
	}
	sleepPath, err := exec.LookPath("sleep")
	require.NoError(t, err)

	t.Run("NotObservedBefore", func(t *testing.T) {
		// The creation time of a process must be the time at
		// which it was created, not the time at which it was
		// first observed. This matters on Linux, where the
		// timestamps of an entry in procfs are set when the
		// entry is first looked up, or looked up again after
		// the kernel has evicted it from its caches. Nothing
		// has looked up the entry of this process yet.
		processID, beforeStart, afterStart := startProcess(t, sleepPath)
		time.Sleep(200 * time.Millisecond)

		creationTime := getProcessCreationTime(t, processID)
		require.False(t, creationTime.After(afterStart), "Process was created at %s, which is after %s", creationTime, afterStart)
		require.False(t, creationTime.Before(beforeStart.Add(-slack)), "Process was created at %s, which is before %s", creationTime, beforeStart)
	})

	t.Run("RepeatedObservations", func(t *testing.T) {
		// A process must not appear to have been created
		// later when it is observed again, as that would
		// cause processes that already existed when bb_runner
		// started, including bb_runner itself, to be killed
		// later on.
		processID, beforeStart, afterStart := startProcess(t, sleepPath)
		for i := 0; i < 5; i++ {
			ownCreationTime := getProcessCreationTime(t, os.Getpid())
			require.False(t, ownCreationTime.After(testStart), "The current process was created at %s, which is after %s", ownCreationTime, testStart)

			creationTime := getProcessCreationTime(t, processID)
			require.False(t, creationTime.After(afterStart), "Process was created at %s, which is after %s", creationTime, afterStart)
			require.False(t, creationTime.Before(beforeStart.Add(-slack)), "Process was created at %s, which is before %s", creationTime, beforeStart)

			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("NotInTheFuture", func(t *testing.T) {
		// No process can have been created after this point.
		// This also holds for the current process, which
		// bb_runner depends on to not terminate itself.
		processes, err := cleaner.SystemProcessTable.GetProcesses()
		require.NoError(t, err)
		now := time.Now()
		for _, process := range processes {
			require.False(t, process.CreationTime.After(now), "Process %d was created at %s, which is after %s", process.ProcessID, process.CreationTime, now)
		}
	})

	t.Run("ExecutableNameWithSpacesAndParentheses", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			return
		}

		// On Linux the start time of a process is stored in
		// /proc/<pid>/stat, after the name of the executable.
		// That name is not escaped, meaning that a process is
		// able to make it resemble the fields that follow it.
		path := filepath.Join(t.TempDir(), "a) R 1 (b")
		require.NoError(t, os.Symlink(sleepPath, path))
		processID, beforeStart, afterStart := startProcess(t, path)

		creationTime := getProcessCreationTime(t, processID)
		require.False(t, creationTime.After(afterStart), "Process was created at %s, which is after %s", creationTime, afterStart)
		require.False(t, creationTime.Before(beforeStart.Add(-slack)), "Process was created at %s, which is before %s", creationTime, beforeStart)
	})
}
