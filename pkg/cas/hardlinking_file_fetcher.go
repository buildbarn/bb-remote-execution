package cas

import (
	"context"
	"os"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type hardlinkingFileFetcher struct {
	base           FileFetcher
	cacheDirectory filesystem.Directory
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

// NewHardlinkingFileFetcher is an adapter for FileFetcher that stores
// files in an internal directory. After successfully downloading files
// at the target location, they are hardlinked into the cache. Future
// calls for the same file will hardlink them from the cache to the
// target location. This reduces the amount of network traffic needed.
func NewHardlinkingFileFetcher(base FileFetcher, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64, evictionSet eviction.Set[string]) FileFetcher {
	return &hardlinkingFileFetcher{
		base:           base,
		cacheDirectory: cacheDirectory,
		maxFiles:       maxFiles,
		maxSize:        maxSize,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,

		downloads: map[string]<-chan struct{}{},
	}
}

func (ff *hardlinkingFileFetcher) makeSpace(size int64) error {
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

func (ff *hardlinkingFileFetcher) GetFile(ctx context.Context, blobDigest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
	key := blobDigest.GetKey(digest.KeyWithoutInstance)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	for {
		// If the file is present in the cache, hardlink it to the destination.
		if err := ff.tryLinkFromCache(key, directory, name); err == nil {
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
				// Download finished. Loop back to try linking from
				// cache. If missing (download failed or other issue),
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
	// tryLinkFromCache() call and acquiring the download lock.
	if err := ff.tryLinkFromCache(key, directory, name); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	downloadErr := ff.base.GetFile(ctx, blobDigest, directory, name, isExecutable)
	if downloadErr == nil {
		// The file was downloaded successfully. Place it into the
		// cache, so that successive calls may use it.
		ff.filesLock.Lock()
		if _, ok := ff.filesSize[key]; !ok {
			ff.evictionLock.Lock()

			// Remove old files from the cache if necessary.
			sizeBytes := blobDigest.GetSizeBytes()
			if err := ff.makeSpace(sizeBytes); err != nil {
				downloadErr = err
			} else {
				// Hardlink the file into the cache.
				if err := directory.Link(name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
					downloadErr = util.StatusWrapfWithCode(err, codes.Internal, "Failed to add cached file %#v", key)
				} else {
					ff.evictionSet.Insert(key)
					ff.filesSize[key] = sizeBytes
					ff.filesTotalSize += sizeBytes
				}
			}
			ff.evictionLock.Unlock()
		} else {
			// The file was already part of our bookkeeping,
			// but was missing on disk. Repair this by adding
			// a link to the newly downloaded file.
			if err := directory.Link(name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
				downloadErr = util.StatusWrapfWithCode(err, codes.Internal, "Failed to repair cached file %#v", key)
			}
		}
		ff.filesLock.Unlock()
	}

	return downloadErr
}

// tryLinkFromCache attempts to create a hardlink from the cache to a
// file in the build directory. It returns os.ErrNotExist if the file
// is not in the cache bookkeeping, or if it was in bookkeeping but
// missing on disk.
func (ff *hardlinkingFileFetcher) tryLinkFromCache(key string, directory filesystem.Directory, name path.Component) error {
	ff.filesLock.RLock()
	_, ok := ff.filesSize[key]
	ff.filesLock.RUnlock()
	if !ok {
		return os.ErrNotExist
	}

	ff.evictionLock.Lock()
	ff.evictionSet.Touch(key)
	ff.evictionLock.Unlock()

	if err := ff.cacheDirectory.Link(path.MustNewComponent(key), directory, name); err == nil {
		// Successfully hardlinked the file to its destination.
		return nil
	} else if !os.IsNotExist(err) {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create hardlink to cached file %#v", key)
	}

	// The file was part of the cache, even though it did not
	// exist on disk. Some other process may have tampered with
	// the cache directory's contents.
	return os.ErrNotExist
}
