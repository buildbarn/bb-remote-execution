package cas

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// CachedFileMaterializer creates a file at a destination from a file at
// a source. cachingFileFetcher uses it in both directions: to add a
// downloaded file to the cache, and to place a cached file in a build
// directory. Errors are returned as is, as callers check them for
// os.IsNotExist() and os.IsExist().
type CachedFileMaterializer func(oldDirectory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error

// LinkCachedFile is a CachedFileMaterializer that creates hardlinks.
// Identical files then share an inode for as long as they remain
// cached.
func LinkCachedFile(oldDirectory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return oldDirectory.Link(oldName, newDirectory, newName)
}

// CloneCachedFile is a CachedFileMaterializer that creates copy-on-write
// clones. Every file placed in a build directory therefore has an inode
// of its own, while still sharing storage with the cache.
//
// On macOS, concurrent build actions sharing one hardlinked input were
// observed locating their own files under another action's build
// directory: Python found its standard library through a sibling's path,
// and imports failed once that sibling's build directory was removed.
// Giving every action an inode of its own prevented it. Cloning also
// avoids linking out of application bundles, which macOS prohibits for
// PkgInfo files.
func CloneCachedFile(oldDirectory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return oldDirectory.Clonefile(oldName, newDirectory, newName)
}

type cachingFileFetcher struct {
	base           FileFetcher
	cacheDirectory filesystem.Directory
	materializer   CachedFileMaterializer
	maxFiles       int
	maxSize        int64

	filesLock      sync.RWMutex
	filesSize      map[string]int64
	filesTotalSize int64

	evictionLock sync.Mutex
	evictionSet  eviction.Set[string]

	downloadsLock sync.Mutex
	downloads     map[string]<-chan struct{}
}

// Runners check readiness concurrently, and multiple build directories
// may be configured with the same cache directory, so every readiness
// check in the process takes a probe name of its own.
var nextReadinessProbeID atomic.Uint64

// NewCachingFileFetcher is an adapter for FileFetcher that stores files
// in an internal directory. After successfully downloading files at the
// target location, they are materialized into the cache. Future calls
// for the same file will materialize them from the cache at the target
// location. This reduces the amount of network traffic needed.
func NewCachingFileFetcher(base FileFetcher, cacheDirectory filesystem.Directory, materializer CachedFileMaterializer, maxFiles int, maxSize int64, evictionSet eviction.Set[string]) FileFetcher {
	return &cachingFileFetcher{
		base:           base,
		cacheDirectory: cacheDirectory,
		materializer:   materializer,
		maxFiles:       maxFiles,
		maxSize:        maxSize,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,

		downloads: map[string]<-chan struct{}{},
	}
}

func (ff *cachingFileFetcher) makeSpace(size int64) error {
	for len(ff.filesSize) > 0 && (len(ff.filesSize) >= ff.maxFiles || ff.filesTotalSize+size > ff.maxSize) {
		// Remove a file from disk.
		key := ff.evictionSet.Peek()
		if err := ff.cacheDirectory.Remove(path.MustNewComponent(key)); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove cached file %#v", key)
		}

		// Remove file from bookkeeping.
		ff.evictionSet.Remove()
		ff.filesTotalSize -= ff.filesSize[key]
		delete(ff.filesSize, key)
	}
	return nil
}

func (ff *cachingFileFetcher) GetFile(ctx context.Context, blobDigest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
	key := blobDigest.GetKey(digest.KeyWithoutInstance)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	for {
		// If the file is present in the cache, materialize it at the destination.
		if err := ff.tryMaterializeFromCache(key, directory, name); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		// A download is required. Let's see if one is already in progress.
		ff.downloadsLock.Lock()
		wait, ok := ff.downloads[key]
		if ok {
			// A download is already in progress. Wait for it to finish.
			ff.downloadsLock.Unlock()
			select {
			case <-wait:
				// Download finished. Loop back to try materializing
				// from cache. If missing (download failed or other issue),
				// we'll attempt a new download.
				continue
			case <-ctx.Done():
				return util.StatusFromContext(ctx)
			}
		}

		// Start a new download.
		break
	}
	newWait := make(chan struct{})
	ff.downloads[key] = newWait
	ff.downloadsLock.Unlock()

	defer func() {
		ff.downloadsLock.Lock()
		delete(ff.downloads, key)
		ff.downloadsLock.Unlock()
		close(newWait)
	}()

	// Check cache again in case another download completed between our initial
	// tryMaterializeFromCache() call and acquiring the download lock.
	if err := ff.tryMaterializeFromCache(key, directory, name); err == nil || !os.IsNotExist(err) {
		return err
	}

	// Download the file at the intended location.
	if err := ff.base.GetFile(ctx, blobDigest, directory, name, isExecutable); err != nil {
		return err
	}

	ff.filesLock.Lock()
	defer ff.filesLock.Unlock()
	if _, ok := ff.filesSize[key]; !ok {
		ff.evictionLock.Lock()
		defer ff.evictionLock.Unlock()

		// Remove old files from the cache if necessary.
		sizeBytes := blobDigest.GetSizeBytes()
		if err := ff.makeSpace(sizeBytes); err != nil {
			return err
		}

		// Add the file to the cache.
		if err := ff.materializer(directory, name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to add cached file %#v", key)
		}
		ff.evictionSet.Insert(key)
		ff.filesSize[key] = sizeBytes
		ff.filesTotalSize += sizeBytes
	} else {
		// Even though the file is part of our bookkeeping, we
		// observed it didn't exist. Repair this inconsistency.
		if err := ff.materializer(directory, name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to repair cached file %#v", key)
		}
	}
	return nil
}

// CheckReadiness verifies that files can be materialized between the
// provided directory and the cache directory in both directions, as
// GetFile() does: into the cache to add a downloaded file, and out of
// the cache to serve a cache hit. Hardlinks and clones both require the
// directories to reside on a single file system, clones additionally
// require one that supports them, and macOS checks the permissions of
// the source and destination separately.
func (ff *cachingFileFetcher) CheckReadiness(ctx context.Context, directory filesystem.Directory) error {
	// The leading dot keeps the probe apart from cache keys, and the
	// cache directory is emptied at startup, which removes probes left
	// behind by a crash.
	probeName := path.MustNewComponent(fmt.Sprintf(".readiness_probe_%d", nextReadinessProbeID.Add(1)))
	f, err := directory.OpenWrite(probeName, filesystem.CreateExcl(0o444))
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create readiness probe file")
	}
	if err := f.Close(); err != nil {
		directory.Remove(probeName)
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to close readiness probe file")
	}

	materializeErr := ff.materializeReadinessProbe(directory, probeName)

	// Remove the probe from both directories, including after a
	// failure, so that build actions and the cache never see it.
	var removeErr error
	for _, probeDirectory := range []filesystem.Directory{directory, ff.cacheDirectory} {
		if err := probeDirectory.Remove(probeName); err != nil && !os.IsNotExist(err) && removeErr == nil {
			removeErr = util.StatusWrapWithCode(err, codes.Internal, "Failed to remove readiness probe file")
		}
	}
	if materializeErr != nil {
		return materializeErr
	}
	return removeErr
}

func (ff *cachingFileFetcher) materializeReadinessProbe(directory filesystem.Directory, probeName path.Component) error {
	if err := ff.materializer(directory, probeName, ff.cacheDirectory, probeName); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to materialize readiness probe file into cache directory")
	}
	if err := directory.Remove(probeName); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to remove readiness probe file")
	}
	if err := ff.materializer(ff.cacheDirectory, probeName, directory, probeName); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to materialize readiness probe file from cache directory")
	}
	return nil
}

// tryMaterializeFromCache attempts to materialize a file from the
// cache in the build directory. It returns os.ErrNotExist if the file
// is not in the cache bookkeeping, or if it was in bookkeeping but
// missing on disk.
func (ff *cachingFileFetcher) tryMaterializeFromCache(key string, directory filesystem.Directory, name path.Component) error {
	ff.filesLock.RLock()
	defer ff.filesLock.RUnlock()

	if _, ok := ff.filesSize[key]; ok {
		ff.evictionLock.Lock()
		ff.evictionSet.Touch(key)
		ff.evictionLock.Unlock()

		if err := ff.materializer(ff.cacheDirectory, path.MustNewComponent(key), directory, name); err == nil {
			// Successfully materialized the file at its destination.
			return nil
		} else if !os.IsNotExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to materialize cached file %#v", key)
		}
	}
	return os.ErrNotExist
}
