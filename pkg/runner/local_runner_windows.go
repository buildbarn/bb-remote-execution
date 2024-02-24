//go:build windows
// +build windows

package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func stripleadingslash(path string) string {
	// TODO: use from bb-storage local_directory_windows.
	// Check for a Linux-style absolute path where the first segment is a drive letter
	// They do not work well on Windows.
	// But where slash paths are accepted, a leading drive letter is okay.
	// Good: C:/tmp
	// Bad: /C:/tmp
	if len(path) >= 4 && path[0] == '/' && path[2] == ':' && path[3] == '/' {
		path = path[1:]
	}

	return path
}

// NewPlainCommandCreator returns a CommandCreator for cases where we don't
// need to chroot into the input root directory.
func NewPlainCommandCreator(sysProcAttr *syscall.SysProcAttr) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryStr, pathVariable string) (*exec.Cmd, error) {
		// exec.CommandContext() has some smartness to call
		// exec.LookPath() under the hood, which we don't want.
		// Call it with a placeholder path, followed by setting
		// cmd.Path and cmd.Args manually. This ensures that our
		// own values remain respected.
		argv0 := arguments[0]
		arguments[0] = "/nonexistent-place-back-later"
		cmd := exec.CommandContext(ctx, "/nonexistent")

		// Set the working relative to be relative to the input
		// root directory.
		workingDirectory, scopeWalker := inputRootDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryStr, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}
		dir := stripleadingslash(workingDirectory.String())

		executablePath, err := lookupExecutable(workingDirectory, pathVariable, argv0)
		if err != nil {
			return nil, err
		}

		// Must use backslashes to execute relative paths
		executablePath = filepath.FromSlash(executablePath)
		arguments[0] = executablePath

		cmd.Args = arguments
		cmd.Path = executablePath
		cmd.SysProcAttr = sysProcAttr
		cmd.Dir = dir
		return cmd, nil
	}
}

// NewChrootedCommandCreator gives an error on Windows, as chroot is not
// supported on the platform.
func NewChrootedCommandCreator(sysProcAttr *syscall.SysProcAttr) (CommandCreator, error) {
	return nil, status.Error(codes.InvalidArgument, "Chroot not supported on Windows")
}

var temporaryDirectoryEnvironmentVariablePrefixes = [...]string{"TMP=", "TEMP="}

var invalidArgumentErrs = [...]error{exec.ErrNotFound, os.ErrPermission, os.ErrNotExist, windows.ERROR_BAD_EXE_FORMAT}

func getPOSIXResourceUsage(cmd *exec.Cmd) *resourceusage.POSIXResourceUsage {
	// TODO: These do not work
	processState := cmd.ProcessState
	return &resourceusage.POSIXResourceUsage{
		UserTime:   durationpb.New(processState.SystemTime()),
		SystemTime: durationpb.New(processState.UserTime()),
	}
}
