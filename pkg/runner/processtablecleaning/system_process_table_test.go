package processtablecleaning_test

import (
	"os"
	"runtime"
	"testing"
	"time"

	ptc "github.com/buildbarn/bb-remote-execution/pkg/runner/processtablecleaning"
	"github.com/stretchr/testify/require"
)

func TestSystemProcessTable(t *testing.T) {
	// TODO: Implement this functionality on non-Linux platforms.
	if runtime.GOOS == "freebsd" {
		return
	}

	processes, err := ptc.SystemProcessTable.GetProcesses()
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
