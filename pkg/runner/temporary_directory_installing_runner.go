package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type temporaryDirectoryInstallingRunner struct {
	base         Runner
	tmpInstaller tmp_installer.TemporaryDirectoryInstallerClient
}

// NewTemporaryDirectoryInstallingRunner creates a Runner that calls
// into a separate gRPC service to announce the availability of a new
// temporary directory that may be used by build actions as a scratch
// space.
//
// This gRPC may, for example, remove the /tmp directory on the system
// and replace it by a symbolic link that points to the directory
// created by the worker.
func NewTemporaryDirectoryInstallingRunner(base Runner, tmpInstaller tmp_installer.TemporaryDirectoryInstallerClient) Runner {
	return &temporaryDirectoryInstallingRunner{
		base:         base,
		tmpInstaller: tmpInstaller,
	}
}

func (r *temporaryDirectoryInstallingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if _, err := r.tmpInstaller.InstallTemporaryDirectory(ctx, &tmp_installer.InstallTemporaryDirectoryRequest{
		TemporaryDirectory: request.TemporaryDirectory,
	}); err != nil {
		return nil, util.StatusWrap(err, "Failed to install temporary directory")
	}
	return r.base.Run(ctx, request)
}
