// +build windows

package runner

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

var temporaryDirectoryEnvironmentVariablePrefixes = [...]string{"TMP=", "TEMP="}

var invalidArgumentErrs = [...]error{exec.ErrNotFound, os.ErrPermission, os.ErrNotExist, windows.ERROR_BAD_EXE_FORMAT}

func checkNewLocalRunnerInput(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, sysProcAttr *syscall.SysProcAttr, setTmpdirEnvironmentVariable, chrootIntoInputRoot bool) error {
	if chrootIntoInputRoot {
		return status.Error(codes.InvalidArgument, "Chroot not supported on Windows")
	}
	return nil
}

func (r *localRunner) copySysProcAttrWithChroot(inputRootDirectory *path.Builder) *syscall.SysProcAttr {
	// If r.chrootIntoInputRoot, then NewLocalRUnner() should fail. We should not reach here.
	return nil
}

func getPOSIXResourceUsage(cmd *exec.Cmd) *resourceusage.POSIXResourceUsage {
	processState := cmd.ProcessState
	return &resourceusage.POSIXResourceUsage{
		UserTime:   durationpb.New(processState.SystemTime()),
		SystemTime: durationpb.New(processState.UserTime()),
	}
}
