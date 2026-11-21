package cas

import (
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

var cloningSupportProbeName = path.MustNewComponent(".clonefile_probe")

// CheckCloningSupported verifies that files can be cloned from a cache
// directory into a build directory, as NewCloningFileFetcher does.
//
// Cloning requires both directories to reside on a single file system
// that supports it (e.g., APFS), which is not the case for every file
// system that supports hardlinks. Probing at startup lets a worker
// refuse a configuration it cannot serve, instead of failing every
// build action it accepts.
func CheckCloningSupported(cacheDirectory filesystem.Directory, cacheDirectoryPath string, buildDirectory filesystem.Directory, buildDirectoryPath string) error {
	// Unlike the cache directory, the build directory is not emptied
	// at startup, so a worker that crashed mid-probe leaves one behind.
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
		if err := buildDirectory.Remove(cloningSupportProbeName); err != nil {
			cacheDirectory.Remove(cloningSupportProbeName)
			return util.StatusWrapf(err, "Failed to remove clone probe file from build directory %#v", buildDirectoryPath)
		}
	}
	if err := cacheDirectory.Remove(cloningSupportProbeName); err != nil {
		return util.StatusWrapf(err, "Failed to remove clone probe file from cache directory %#v", cacheDirectoryPath)
	}
	if cloneErr != nil {
		return util.StatusWrapfWithCode(cloneErr, codes.FailedPrecondition, "Cannot clone files from cache directory %#v into build directory %#v", cacheDirectoryPath, buildDirectoryPath)
	}
	return nil
}
