package builder

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/atomic"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
)

type sharedBuildDirectoryCreator struct {
	base                 BuildDirectoryCreator
	nextParallelActionID *atomic.Uint64
}

var (
	buildDirectoryPrometheusMetrics sync.Once

	buildDirectoryClosingDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "build_directory",
			Name:      "build_directory_closing_duration_seconds",
			Help:      "Amount of time spent in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		})
)

// NewSharedBuildDirectoryCreator is an adapter for
// BuildDirectoryCreator that causes build actions to be executed inside
// a subdirectory within the build directory, as opposed to inside the
// build directory itself. The subdirectory is either named after the
// action digest of the build action or uses an incrementing number,
// based on whether collisions may occur.
//
// This adapter can be used to add concurrency to a single worker. When
// executing build actions in parallel, every build action needs its own
// build directory.
func NewSharedBuildDirectoryCreator(base BuildDirectoryCreator, nextParallelActionID *atomic.Uint64) BuildDirectoryCreator {
	buildDirectoryPrometheusMetrics.Do(func() {
		prometheus.MustRegister(buildDirectoryClosingDurationSeconds)
	})

	return &sharedBuildDirectoryCreator{
		base:                 base,
		nextParallelActionID: nextParallelActionID,
	}
}

func (dc *sharedBuildDirectoryCreator) GetBuildDirectory(ctx context.Context, actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, *path.Trace, error) {
	parentDirectory, parentDirectoryPath, err := dc.base.GetBuildDirectory(ctx, actionDigest, mayRunInParallel)
	if err != nil {
		return nil, nil, err
	}

	// Determine the name of the subdirectory.
	var name string
	if mayRunInParallel {
		// Multiple instances of this action may run in
		// parallel, as the scheduler is not permitted to
		// deduplicate them. This is likely caused by the
		// 'do_not_cache' flag being set in the Action message.
		//
		// Number subdirectories incrementally to prevent
		// collisions if multiple of them are scheduled on the
		// same worker.
		name = strconv.FormatUint(dc.nextParallelActionID.Add(1), 10)
	} else {
		// This action is guaranteed not to run in parallel, due
		// to the scheduler being permitted to deduplicate
		// execution requests. Use a directory name based on the
		// action digest. This ensures that the working
		// directory of the build action is deterministic,
		// thereby increasing reproducibility.
		//
		// Only use a small number of characters from the digest
		// to ensure the absolute path of the build directory
		// remains short. This avoids reaching PATH_MAX and
		// sockaddr_un::sun_path size limits for stronger digest
		// functions. 16 characters is more than sufficient to
		// prevent collisions.
		name = actionDigest.GetHashString()[:16]
	}

	// Create the subdirectory.
	childDirectoryName := path.MustNewComponent(name)
	childDirectoryPath := parentDirectoryPath.Append(childDirectoryName)
	if err := parentDirectory.Mkdir(childDirectoryName, 0o777); err != nil {
		parentDirectory.Close()
		return nil, nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create build directory %#v", childDirectoryPath.String())
	}
	childDirectory, err := parentDirectory.EnterBuildDirectory(childDirectoryName)
	if err != nil {
		if err := parentDirectory.Remove(childDirectoryName); err != nil {
			log.Printf("Failed to remove action digest build directory %#v upon failure to enter: %s", childDirectoryPath.String(), err)
		}
		parentDirectory.Close()
		return nil, nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter build directory %#v", childDirectoryPath.String())
	}

	return &sharedBuildDirectory{
		BuildDirectory:     childDirectory,
		parentDirectory:    parentDirectory,
		childDirectoryName: childDirectoryName,
		childDirectoryPath: childDirectoryPath.String(),
	}, childDirectoryPath, nil
}

type sharedBuildDirectory struct {
	BuildDirectory
	parentDirectory    BuildDirectory
	childDirectoryName path.Component
	childDirectoryPath string
}

func (d *sharedBuildDirectory) Close() error {
	timeStart := time.Now()
	
	err1 := d.BuildDirectory.Close()
	err2 := d.parentDirectory.RemoveAll(d.childDirectoryName)
	err3 := d.parentDirectory.Close()

	buildDirectoryClosingDurationSeconds.Observe(time.Now().Sub(timeStart).Seconds())

	if err1 != nil {
		return util.StatusWrapf(err1, "Failed to close build directory %#v", d.childDirectoryPath)
	}
	if err2 != nil {
		return util.StatusWrapfWithCode(err2, codes.Internal, "Failed to remove build directory %#v", d.childDirectoryPath)
	}
	return err3
}
