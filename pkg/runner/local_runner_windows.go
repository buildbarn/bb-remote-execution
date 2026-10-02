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
	return nil, status.Error(codes.InvalidArgument, "Chroot is not supported on Windows")
}

var temporaryDirectoryEnvironmentVariablePrefixes = [...]string{"TMP=", "TEMP="}

var invalidArgumentErrs = [...]error{exec.ErrNotFound, os.ErrPermission, os.ErrNotExist, windows.ERROR_BAD_EXE_FORMAT}

func getPOSIXResourceUsage(cmd *exec.Cmd) *resourceusage.POSIXResourceUsage {
	// TODO: These do not work.
	processState := cmd.ProcessState
	return &resourceusage.POSIXResourceUsage{
		UserTime:   durationpb.New(processState.SystemTime()),
		SystemTime: durationpb.New(processState.UserTime()),
	}
}

// sysProcAttrForArguments returns the SysProcAttr to use for running an
// action. For invocations of cmd.exe that carry the script as a single
// argument, an explicit command line is provided, bypassing the escaping
// that Go would otherwise apply.
func sysProcAttrForArguments(sysProcAttr *syscall.SysProcAttr, arguments []string) *syscall.SysProcAttr {
	cmdLine, ok := windowsCommandLineForCmdExe(arguments)
	if !ok {
		return sysProcAttr
	}
	// The same SysProcAttr is handed to every action, so the command
	// line of a single one may not be stored in it.
	var sysProcAttrCopy syscall.SysProcAttr
	if sysProcAttr != nil {
		sysProcAttrCopy = *sysProcAttr
	}
	sysProcAttrCopy.CmdLine = cmdLine
	return &sysProcAttrCopy
}
