package builder

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type rootBuildDirectoryCreator struct {
	buildDirectory BuildDirectory
}

// NewRootBuildDirectoryCreator is a BuildDirectoryCreator that
// repeatedly hands out a single directory present on the current
// system. Additional decorators are used to run builds in
// subdirectories, so that build actions may run in parallel.
func NewRootBuildDirectoryCreator(buildDirectory BuildDirectory) BuildDirectoryCreator {
	dc := &rootBuildDirectoryCreator{
		buildDirectory: rootBuildDirectory{
			BuildDirectory: buildDirectory,
		},
	}
	return dc
}

func (dc *rootBuildDirectoryCreator) GetBuildDirectory(actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, *path.Trace, error) {
	return dc.buildDirectory, nil, nil
}

type rootBuildDirectory struct {
	BuildDirectory
}

func (d rootBuildDirectory) Close() error {
	// Never call Close() on the root directory, as it will be reused.
	return nil
}
