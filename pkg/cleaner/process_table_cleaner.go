package cleaner

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewProcessTableCleaner creates a decorator for Runner that kills
// processes that were left behind by previous build actions (e.g.,
// daemonized processes).
func NewProcessTableCleaner(processTable ProcessTable) Cleaner {
	return func(ctx context.Context) error {
		processes, err := processTable.GetProcesses()
		if err != nil {
			return util.StatusWrap(err, "Failed to get processes from process table")
		}
		for _, process := range processes {
			if err := killProcess(int(process.ProcessID)); err != nil {
				return util.StatusWrapf(err, "Failed to kill process %d", process.ProcessID)
			}
		}
		return nil
	}
}
