// +build darwin

package processtablecleaning

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
)

type systemProcessTable struct{}

func (pt systemProcessTable) GetProcesses() ([]Process, error) {
	kinfoProcs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to obtain process table")
	}

	processes := make([]Process, 0, len(kinfoProcs))
	for _, kinfoProc := range kinfoProcs {
		startTime := kinfoProc.Proc.P_starttime
		processes = append(processes, Process{
			ProcessID:    int(kinfoProc.Proc.P_pid),
			UserID:       int(kinfoProc.Eproc.Ucred.Uid),
			CreationTime: time.Unix(startTime.Sec, int64(startTime.Usec)*1000),
		})
	}
	return processes, nil
}

// SystemProcessTable corresponds with the process table of the locally
// running operating system. On this operating system the information is
// extracted from the "kern.proc.all" sysctl.
var SystemProcessTable ProcessTable = systemProcessTable{}
