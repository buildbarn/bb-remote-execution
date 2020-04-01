package builder

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BuildDirectoryCreator is used by LocalBuildExecutor to obtain build
// directories in which build actions are executed.
type BuildDirectoryCreator interface {
	GetBuildDirectory(actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, string, error)
}
