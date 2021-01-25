// +build windows

package runner

import (
	"context"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localRunner struct{}

// NewLocalRunner returns a Runner capable of running commands on the
// local system directly. On this operating system this functionality is
// not available.
func NewLocalRunner(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, sysProcAttr *syscall.SysProcAttr, setTmpdirEnvironmentVariable, chrootIntoInputRoot bool) Runner {
	return &localRunner{}
}

func (r *localRunner) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Local process execution is not supported on this platform")
}
