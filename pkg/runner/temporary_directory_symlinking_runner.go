package runner

import (
	"context"
	"os"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type temporaryDirectorySymlinkingRunner struct {
	base               Runner
	symlinkPath        string
	buildDirectoryPath *path.Builder
}

// NewTemporaryDirectorySymlinkingRunner creates a decorator for Runner
// that removes a local path on the system and replaces it with a
// symbolic link pointing to the temporary directory that was created by
// bb_worker as part of the action's build directory.
func NewTemporaryDirectorySymlinkingRunner(base Runner, symlinkPath string, buildDirectoryPath *path.Builder) Runner {
	return &temporaryDirectorySymlinkingRunner{
		base:               base,
		symlinkPath:        symlinkPath,
		buildDirectoryPath: buildDirectoryPath,
	}
}

func (r *temporaryDirectorySymlinkingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	// Compute the absolute path of the temporary directory that is
	// offered by bb_worker.
	temporaryDirectoryPath, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
	}

	// Install a symbolic link pointing to the temporary directory.
	if err := os.Remove(r.symlinkPath); err != nil && !os.IsNotExist(err) {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove symbolic link %#v", r.symlinkPath)
	}
	symlinkTarget := temporaryDirectoryPath.String()
	if err := os.Symlink(symlinkTarget, r.symlinkPath); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create symbolic link %#v pointing to %#v", r.symlinkPath, symlinkTarget)
	}

	return r.base.Run(ctx, request)
}
