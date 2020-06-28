package processtablecleaning

import (
	"context"
	"syscall"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

type processTableCleaningRunner struct {
	base         runner.Runner
	processTable ProcessTable
	initializer  sync.Initializer
}

// NewProcessTableCleaningRunner creates a decorator for Runner that
// kills processes that were left behind by previous build actions
// (e.g., daemonized processes).
func NewProcessTableCleaningRunner(base runner.Runner, processTable ProcessTable) runner.Runner {
	return &processTableCleaningRunner{
		base:         base,
		processTable: processTable,
	}
}

func (r *processTableCleaningRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if err := r.initializer.Acquire(func() error {
		processes, err := r.processTable.GetProcesses()
		if err != nil {
			return util.StatusWrap(err, "Failed to get processes from process table")
		}
		for _, process := range processes {
			if err := unix.Kill(int(process.ProcessID), syscall.SIGKILL); err != nil && err != syscall.ESRCH {
				return util.StatusWrapf(err, "Failed to kill process %d", process.ProcessID)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	defer r.initializer.Release()
	return r.base.Run(ctx, request)
}
