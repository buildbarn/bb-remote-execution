//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package runner

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NewChrootedCommandCreator returns a CommandCreator for cases where we
// need to chroot into the input root directory.
func NewChrootedCommandCreator(sysProcAttr *syscall.SysProcAttr) (CommandCreator, error) {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryParser path.Parser, pathVariable string) (*exec.Cmd, error) {
		// The addition of /usr/bin/env is necessary as the PATH resolution
		// will take place prior to the chroot, so the executable may not be
		// found by exec.LookPath() inside exec.CommandContext() and may
		// cause cmd.Start() to fail when it shouldn't.
		// https://github.com/golang/go/issues/39341
		cmd := exec.CommandContext(ctx, "/usr/bin/env", append([]string{"--"}, arguments...)...)

		inputRootDirectoryStr, err := path.LocalFormat.GetString(inputRootDirectory)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create local representation of input root directory")
		}
		sysProcAttrCopy := *sysProcAttr
		sysProcAttrCopy.Chroot = inputRootDirectoryStr
		cmd.SysProcAttr = &sysProcAttrCopy

		// Set the working relative to be relative to the root
		// directory of the chrooted environment.
		workingDirectory, scopeWalker := path.RootBuilder.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryParser, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}
		workingDirectoryStr, err := path.LocalFormat.GetString(workingDirectory)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create local representation of working directory")
		}
		cmd.Dir = workingDirectoryStr
		return cmd, nil
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
	resourceUsage := &resourceusage.POSIXResourceUsage{
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
	if waitStatus := cmd.ProcessState.Sys().(syscall.WaitStatus); waitStatus.Signaled() {
		if s, ok := strings.CutPrefix(unix.SignalName(waitStatus.Signal()), "SIG"); ok {
			resourceUsage.TerminationSignal = s
		}
	}
	return resourceUsage
}
