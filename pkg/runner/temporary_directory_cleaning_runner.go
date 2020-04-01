package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type temporaryDirectoryCleaningRunner struct {
	base          Runner
	directory     filesystem.Directory
	directoryPath string

	initializer sync.Initializer
}

// NewTemporaryDirectoryCleaningRunner is an adapter for Runner that
// empties out a temporary directory (e.g. /tmp, /var/tmp, etc) when
// transitioning from idle to being used. This ensures that build
// actions are executed in a clean environment.
func NewTemporaryDirectoryCleaningRunner(base Runner, directory filesystem.Directory, directoryPath string) Runner {
	return &temporaryDirectoryCleaningRunner{
		base:          base,
		directory:     directory,
		directoryPath: directoryPath,
	}
}

func (r *temporaryDirectoryCleaningRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if err := r.initializer.Acquire(r.directory.RemoveAllChildren); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to clean temporary directory %#v prior to build", r.directoryPath)
	}
	// TODO: Should we also trigger a cleanup when going back to
	// idle, so that files don't remain on disk longer than
	// necessary?
	defer r.initializer.Release()
	return r.base.Run(ctx, request)
}
