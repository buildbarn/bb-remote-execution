package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// BuildDirectoryCreator is used by LocalBuildExecutor to obtain build
// directories in which build actions are executed.
type BuildDirectoryCreator interface {
	GetBuildDirectory(ctx context.Context, actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, *path.Trace, error)
}
