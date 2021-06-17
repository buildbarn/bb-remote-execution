package builder

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type cleanBuildDirectoryCreator struct {
	base        BuildDirectoryCreator
	idleInvoker *cleaner.IdleInvoker
}

// NewCleanBuildDirectoryCreator is an adapter for BuildDirectoryCreator
// that upon acquistion and release calls into a Cleaner. This Cleaner
// may, for example, be set up to empty out the build directory. This
// guarantees that build actions aren't able to see data left behind by
// ones that ran previously.
func NewCleanBuildDirectoryCreator(base BuildDirectoryCreator, idleInvoker *cleaner.IdleInvoker) BuildDirectoryCreator {
	return &cleanBuildDirectoryCreator{
		base:        base,
		idleInvoker: idleInvoker,
	}
}

func (dc *cleanBuildDirectoryCreator) GetBuildDirectory(ctx context.Context, actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, *path.Trace, error) {
	if err := dc.idleInvoker.Acquire(ctx); err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to clean before acquiring build directory")
	}
	buildDirectory, buildDirectoryPath, err := dc.base.GetBuildDirectory(ctx, actionDigest, mayRunInParallel)
	if err != nil {
		dc.idleInvoker.Release(ctx)
		return nil, nil, err
	}
	return &cleanBuildDirectory{
		BuildDirectory: buildDirectory,
		idleInvoker:    dc.idleInvoker,
		context:        ctx,
	}, buildDirectoryPath, nil
}

type cleanBuildDirectory struct {
	BuildDirectory
	idleInvoker *cleaner.IdleInvoker
	context     context.Context
}

func (d cleanBuildDirectory) Close() error {
	err1 := d.BuildDirectory.Close()
	err2 := d.idleInvoker.Release(d.context)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return util.StatusWrap(err2, "Failed to clean after releasing build directory")
	}
	return nil
}
