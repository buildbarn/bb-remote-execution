package builder

import (
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type cleanBuildDirectoryCreator struct {
	base        BuildDirectoryCreator
	initializer *sync.Initializer
}

// NewCleanBuildDirectoryCreator is an adapter for BuildDirectoryCreator
// that upon acquistion empties out the build directory. This ensures
// that the build action is executed in a clean environment.
func NewCleanBuildDirectoryCreator(base BuildDirectoryCreator, initializer *sync.Initializer) BuildDirectoryCreator {
	return &cleanBuildDirectoryCreator{
		base:        base,
		initializer: initializer,
	}
}

func (dc *cleanBuildDirectoryCreator) GetBuildDirectory(actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, string, error) {
	buildDirectory, buildDirectoryPath, err := dc.base.GetBuildDirectory(actionDigest, mayRunInParallel)
	if err != nil {
		return nil, "", err
	}
	if err := dc.initializer.Acquire(buildDirectory.RemoveAllChildren); err != nil {
		buildDirectory.Close()
		return nil, "", util.StatusWrapfWithCode(err, codes.Internal, "Failed to clean build directory %#v prior to build", buildDirectoryPath)
	}
	return &cleanBuildDirectory{
		BuildDirectory: buildDirectory,
		initializer:    dc.initializer,
	}, buildDirectoryPath, nil
}

type cleanBuildDirectory struct {
	BuildDirectory
	initializer *sync.Initializer
}

func (d cleanBuildDirectory) Close() error {
	err := d.BuildDirectory.Close()
	d.initializer.Release()
	return err
}
