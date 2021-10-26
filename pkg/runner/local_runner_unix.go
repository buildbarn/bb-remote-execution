// +build darwin freebsd linux

package runner

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/protobuf/types/known/durationpb"
)

var temporaryDirectoryEnvironmentVariablePrefixes = [...]string{"TMPDIR="}

var invalidArgumentErrs = [...]error{exec.ErrNotFound, os.ErrPermission, syscall.ENOENT, syscall.ENOEXEC}

func checkNewLocalRunnerInput(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, sysProcAttr *syscall.SysProcAttr, setTmpdirEnvironmentVariable, chrootIntoInputRoot bool) error {
	return nil
}

func (r *localRunner) copySysProcAttrWithChroot(inputRootDirectory *path.Builder) *syscall.SysProcAttr {
	sysProcAttr := *r.sysProcAttr
	sysProcAttr.Chroot = inputRootDirectory.String()
	return &sysProcAttr
}

func convertTimeval(t syscall.Timeval) *durationpb.Duration {
	return &durationpb.Duration{
		Seconds: int64(t.Sec),
		Nanos:   int32(t.Usec) * 1000,
	}
}

func getPOSIXResourceUsage(cmd *exec.Cmd) *resourceusage.POSIXResourceUsage {
	rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	return &resourceusage.POSIXResourceUsage{
		UserTime:                   convertTimeval(rusage.Utime),
		SystemTime:                 convertTimeval(rusage.Stime),
		MaximumResidentSetSize:     int64(rusage.Maxrss) * maximumResidentSetSizeUnit,
		PageReclaims:               int64(rusage.Minflt),
		PageFaults:                 int64(rusage.Majflt),
		Swaps:                      int64(rusage.Nswap),
		BlockInputOperations:       int64(rusage.Inblock),
		BlockOutputOperations:      int64(rusage.Oublock),
		MessagesSent:               int64(rusage.Msgsnd),
		MessagesReceived:           int64(rusage.Msgrcv),
		SignalsReceived:            int64(rusage.Nsignals),
		VoluntaryContextSwitches:   int64(rusage.Nvcsw),
		InvoluntaryContextSwitches: int64(rusage.Nivcsw),
	}
}
