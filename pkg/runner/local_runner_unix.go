//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// lookupExecutable returns the path of an executable, taking the PATH
// environment variable into account.
func lookupExecutable(argv0 string, workingDirectory *path.Builder, pathVariable string) (*path.Builder, error) {
	if strings.ContainsRune(argv0, os.PathSeparator) {
		// The path contains one or more slashes. Resolve it to
		// a location relative to the action's working
		// directory.
		executablePath, scopeWalker := workingDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(argv0, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve executable path")
		}
		return executablePath, nil
	}

	// Executable path does not contain any slashes. Perform PATH
	// lookups.
	//
	// We cannot use exec.LookPath() directly, as that function
	// disregards the working directory of the action. It also uses
	// the PATH environment variable of the current process, as
	// opposed to respecting the value that is provided as part of
	// the action. Do call into this function to validate the
	// existence of the executable.
	for _, searchPathStr := range filepath.SplitList(pathVariable) {
		searchPath, scopeWalker := workingDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(searchPathStr, scopeWalker); err != nil {
			return nil, util.StatusWrapf(err, "Failed to resolve executable search path %#v", searchPathStr)
		}
		executablePath, scopeWalker := searchPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(argv0, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve executable path")
		}
		if _, err := exec.LookPath(executablePath.String()); err == nil {
			return executablePath, nil
		}
	}
	return nil, status.Errorf(codes.InvalidArgument, "Cannot find executable %#v in search paths %#v", argv0, pathVariable)
}

// NewPlainCommandCreator returns a CommandCreator for cases where we don't
// need to chroot into the input root directory.
func NewPlainCommandCreator(sysProcAttr *syscall.SysProcAttr) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryStr, pathVariable string) (*exec.Cmd, error) {
		workingDirectory, scopeWalker := inputRootDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryStr, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}

		// Call exec.CommandContext() with an absolute path, so
		// that it does not try to do PATH lookups for us.
		// Override the arguments afterwards, so that argv[0]
		// remains equal to the originally provided value.
		executablePath, err := lookupExecutable(arguments[0], workingDirectory, pathVariable)
		if err != nil {
			return nil, err
		}
		cmd := exec.CommandContext(ctx, executablePath.String())
		cmd.Args = arguments
		cmd.SysProcAttr = sysProcAttr

		cmd.Dir = workingDirectory.String()
		return cmd, nil
	}
}

// NewChrootedCommandCreator returns a CommandCreator for cases where we
// need to chroot into the input root directory.
func NewChrootedCommandCreator(sysProcAttr *syscall.SysProcAttr) (CommandCreator, error) {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryStr, pathVariable string) (*exec.Cmd, error) {
		// The addition of /usr/bin/env is necessary as the PATH resolution
		// will take place prior to the chroot, so the executable may not be
		// found by exec.LookPath() inside exec.CommandContext() and may
		// cause cmd.Start() to fail when it shouldn't.
		// https://github.com/golang/go/issues/39341
		cmd := exec.CommandContext(ctx, "/usr/bin/env", append([]string{"--"}, arguments...)...)
		sysProcAttrCopy := *sysProcAttr
		sysProcAttrCopy.Chroot = inputRootDirectory.String()
		cmd.SysProcAttr = &sysProcAttrCopy

		// Set the working relative to be relative to the root
		// directory of the chrooted environment.
		workingDirectory, scopeWalker := path.RootBuilder.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryStr, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}
		cmd.Dir = workingDirectory.String()
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
