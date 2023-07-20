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

// getExecutablePath returns the path of an executable within a given
// search path that is part of the PATH environment variable.
func getExecutablePath(baseDirectory *path.Builder, searchPathStr, argv0 string) (string, error) {
	searchPath, scopeWalker := baseDirectory.Join(path.VoidScopeWalker)
	if err := path.Resolve(searchPathStr, scopeWalker); err != nil {
		return "", err
	}
	executablePath, scopeWalker := searchPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(argv0, scopeWalker); err != nil {
		return "", err
	}
	return executablePath.String(), nil
}

// lookupExecutable returns the path of an executable, taking the PATH
// environment variable into account.
func lookupExecutable(workingDirectory *path.Builder, pathVariable, argv0 string) (string, error) {
	if strings.ContainsRune(argv0, os.PathSeparator) {
		// No PATH processing needs to be performed.
		return argv0, nil
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
		executablePathAbs, err := getExecutablePath(workingDirectory, searchPathStr, argv0)
		if err != nil {
			return "", util.StatusWrapf(err, "Failed to resolve executable %#v in search path %#v", argv0, searchPathStr)
		}
		if _, err := exec.LookPath(executablePathAbs); err == nil {
			// Regular compiled executables will receive the
			// argv[0] that we provide, but scripts starting
			// with '#!' will receive the literal executable
			// path.
			//
			// Most shells seem to guarantee that if argv[0]
			// is relative, the executable path is relative
			// as well. Prevent these scripts from breaking
			// by recomputing the executable path once more,
			// but relative.
			executablePathRel, err := getExecutablePath(&path.EmptyBuilder, searchPathStr, argv0)
			if err != nil {
				return "", util.StatusWrapf(err, "Failed to resolve executable %#v in search path %#v", argv0, searchPathStr)
			}
			return executablePathRel, nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "Cannot find executable %#v in search paths %#v", argv0, pathVariable)
}

// NewPlainCommandCreator returns a CommandCreator for cases where we don't
// need to chroot into the input root directory.
func NewPlainCommandCreator(sysProcAttr *syscall.SysProcAttr) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryStr, pathVariable string) (*exec.Cmd, error) {
		workingDirectory, scopeWalker := inputRootDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryStr, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}
		executablePath, err := lookupExecutable(workingDirectory, pathVariable, arguments[0])
		if err != nil {
			return nil, err
		}

		// exec.CommandContext() has some smartness to call
		// exec.LookPath() under the hood, which we don't want.
		// Call it with a placeholder path, followed by setting
		// cmd.Path and cmd.Args manually. This ensures that our
		// own values remain respected.
		cmd := exec.CommandContext(ctx, "/nonexistent")
		cmd.Args = arguments
		cmd.Dir = workingDirectory.String()
		cmd.Path = executablePath
		cmd.SysProcAttr = sysProcAttr
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
