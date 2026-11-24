package cas

import (
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

var (
	cloningSupportProbeName         = path.MustNewComponent(".clonefile_probe")
	cloningSupportReturnedProbeName = path.MustNewComponent(".clonefile_probe_returned")
)

// CheckCloningSupported verifies that files can be cloned between a
// cache directory and a build directory in both directions, as a
// caching FileFetcher using CloneCachedFile does: from the cache to serve a cache hit, and
// into the cache to add or repair a downloaded file.
//
// Cloning requires both directories to reside on a single file system
// that supports it (e.g., APFS), which is not the case for every file
// system that supports hardlinks, and macOS checks permissions on the
// source and destination separately. Probing at startup lets a worker
// refuse a configuration it cannot serve, instead of failing every
// build action it accepts.
func CheckCloningSupported(cacheDirectory filesystem.Directory, cacheDirectoryPath string, buildDirectory filesystem.Directory, buildDirectoryPath string) error {
	// The cache directory is emptied at startup, but the build
	// directory is only emptied before its first build action, so a
	// worker that crashed mid-probe leaves one behind there. Nothing
	// else in the build directory survives that emptying, so removing
	// it discards nothing of value.
	if err := buildDirectory.Remove(cloningSupportProbeName); err != nil && !os.IsNotExist(err) {
		return util.StatusWrapf(err, "Failed to remove stale clone probe file from build directory %#v", buildDirectoryPath)
	}
	f, err := cacheDirectory.OpenWrite(cloningSupportProbeName, filesystem.CreateExcl(0o444))
	if err != nil {
		return util.StatusWrapf(err, "Failed to create clone probe file in cache directory %#v", cacheDirectoryPath)
	}
	if err := f.Close(); err != nil {
		cacheDirectory.Remove(cloningSupportProbeName)
		return util.StatusWrapf(err, "Failed to close clone probe file in cache directory %#v", cacheDirectoryPath)
	}

	cloneErr := cacheDirectory.Clonefile(cloningSupportProbeName, buildDirectory, cloningSupportProbeName)
	if cloneErr == nil {
		cloneErr = buildDirectory.Clonefile(cloningSupportProbeName, cacheDirectory, cloningSupportReturnedProbeName)
	}

	// Remove every probe file, including after a failed clone, so
	// that none is left for build actions to see.
	var removeErr error
	for _, probe := range []struct {
		directory     filesystem.Directory
		directoryPath string
		name          path.Component
	}{
		{buildDirectory, buildDirectoryPath, cloningSupportProbeName},
		{cacheDirectory, cacheDirectoryPath, cloningSupportReturnedProbeName},
		{cacheDirectory, cacheDirectoryPath, cloningSupportProbeName},
	} {
		if err := probe.directory.Remove(probe.name); err != nil && !os.IsNotExist(err) && removeErr == nil {
			removeErr = util.StatusWrapf(err, "Failed to remove clone probe file %#v from directory %#v", probe.name.String(), probe.directoryPath)
		}
	}

	if cloneErr != nil {
		return util.StatusWrapfWithCode(cloneErr, codes.FailedPrecondition, "Cannot clone files between cache directory %#v and build directory %#v", cacheDirectoryPath, buildDirectoryPath)
	}
	return removeErr
}
