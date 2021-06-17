package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/emptypb"
)

type temporaryDirectoryInstallingRunner struct {
	base         runner_pb.RunnerServer
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
func NewTemporaryDirectoryInstallingRunner(base runner_pb.RunnerServer, tmpInstaller tmp_installer.TemporaryDirectoryInstallerClient) runner_pb.RunnerServer {
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

func (r *temporaryDirectoryInstallingRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	if _, err := r.tmpInstaller.CheckReadiness(ctx, &emptypb.Empty{}); err != nil {
		return nil, util.StatusWrap(err, "Readiness check of temporary directory installer failed")
	}
	return r.base.CheckReadiness(ctx, request)
}
