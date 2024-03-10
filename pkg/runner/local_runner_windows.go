//go:build windows
// +build windows

package runner

import (
	"context"
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NewPlainCommandCreator returns a CommandCreator for cases where we don't
// need to chroot into the input root directory.
func NewPlainCommandCreator(sysProcAttr *syscall.SysProcAttr) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryParser path.Parser, pathVariable string) (*exec.Cmd, error) {
		// TODO: This may not work correctly if the action sets
		// the PATH environment variable explicitly.
		cmd := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
		cmd.SysProcAttr = sysProcAttr

		// Set the working relative to be relative to the input
		// root directory.
		workingDirectory, scopeWalker := inputRootDirectory.Join(path.VoidScopeWalker)
		if err := path.Resolve(workingDirectoryParser, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve working directory")
		}
		workingDirectoryStr, err := path.GetLocalString(workingDirectory)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create local representation of working directory")
		}
		cmd.Dir = workingDirectoryStr
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
	processState := cmd.ProcessState
	return &resourceusage.POSIXResourceUsage{
		UserTime:   durationpb.New(processState.SystemTime()),
		SystemTime: durationpb.New(processState.UserTime()),
	}
}
