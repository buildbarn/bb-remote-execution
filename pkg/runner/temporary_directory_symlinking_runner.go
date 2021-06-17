package runner

import (
	"context"
	"os"
	"sync"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

type temporaryDirectorySymlinkingRunner struct {
	base               runner_pb.RunnerServer
	symlinkPath        string
	buildDirectoryPath *path.Builder

	lock     sync.Mutex
	runCount int
}

// NewTemporaryDirectorySymlinkingRunner creates a decorator for Runner
// that removes a local path on the system and replaces it with a
// symbolic link pointing to the temporary directory that was created by
// bb_worker as part of the action's build directory.
func NewTemporaryDirectorySymlinkingRunner(base runner_pb.RunnerServer, symlinkPath string, buildDirectoryPath *path.Builder) runner_pb.RunnerServer {
	return &temporaryDirectorySymlinkingRunner{
		base:               base,
		symlinkPath:        symlinkPath,
		buildDirectoryPath: buildDirectoryPath,
	}
}

func (r *temporaryDirectorySymlinkingRunner) updateSymlink(symlinkTarget string) error {
	if err := os.Remove(r.symlinkPath); err != nil && !os.IsNotExist(err) {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove symbolic link %#v", r.symlinkPath)
	}
	if err := os.Symlink(symlinkTarget, r.symlinkPath); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create symbolic link %#v pointing to %#v", r.symlinkPath, symlinkTarget)
	}
	return nil
}

func (r *temporaryDirectorySymlinkingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	// Keep track of the number of concurrent Run() calls. When
	// zero, CheckReadiness() can safely adjust the symbolic link
	// for testing.
	r.lock.Lock()
	r.runCount++
	r.lock.Unlock()
	defer func() {
		r.lock.Lock()
		r.runCount--
		r.lock.Unlock()
	}()

	// Compute the absolute path of the temporary directory that is
	// offered by bb_worker.
	temporaryDirectoryPath, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
	}

	// Install a symbolic link pointing to the temporary directory.
	if err := r.updateSymlink(temporaryDirectoryPath.String()); err != nil {
		return nil, err
	}
	return r.base.Run(ctx, request)
}

func (r *temporaryDirectorySymlinkingRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	// When idle, test that symlink creation works properly. That
	// way the worker won't pick up any actions from the scheduler
	// in case of misconfigurations.
	r.lock.Lock()
	if r.runCount == 0 {
		if err := r.updateSymlink("/nonexistent"); err != nil {
			r.lock.Unlock()
			return nil, err
		}
	}
	r.lock.Unlock()

	return r.base.CheckReadiness(ctx, request)
}
