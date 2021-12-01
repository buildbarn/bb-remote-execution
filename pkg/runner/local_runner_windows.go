//go:build windows
// +build windows

package runner

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
