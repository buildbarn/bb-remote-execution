//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package runner

import (
	"context"
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/protobuf/types/known/durationpb"
)

// NewChrootedCommandCreator returns a CommandCreator for cases where we
// need to chroot into the input root directory.
func NewChrootedCommandCreator(sysProcAttr *syscall.SysProcAttr) (CommandCreator, error) {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder) (*exec.Cmd, *path.Builder) {
		// The addition of /usr/bin/env is necessary as the PATH resolution
		// will take place prior to the chroot, so the executable may not be
		// found by exec.LookPath() inside exec.CommandContext() and may
		// cause cmd.Start() to fail when it shouldn't.
		// https://github.com/golang/go/issues/39341
		cmd := exec.CommandContext(ctx, "/usr/bin/env", append([]string{"--"}, arguments...)...)
		sysProcAttrCopy := *sysProcAttr
		sysProcAttrCopy.Chroot = inputRootDirectory.String()
		cmd.SysProcAttr = &sysProcAttrCopy
		return cmd, &path.RootBuilder
	}, nil
}

var temporaryDirectoryEnvironmentVariablePrefixes = [...]string{"TMPDIR="}

var invalidArgumentErrs = []error{exec.ErrNotFound, os.ErrPermission, syscall.EISDIR, syscall.ENOENT, syscall.ENOEXEC}

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
