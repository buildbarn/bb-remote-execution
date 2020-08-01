package cas

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type hardlinkingFileFetcher struct {
	base            FileFetcher
	digestKeyFormat digest.KeyFormat
	cacheDirectory  filesystem.Directory
	maxFiles        int
	maxSize         int64

	filesLock      sync.RWMutex
	filesSize      map[string]int64
	filesTotalSize int64

	evictionLock sync.Mutex
	evictionSet  eviction.Set
}

// NewHardlinkingFileFetcher is an adapter for FileFetcher that stores
// files in an internal directory. After successfully downloading files
// at the target location, they are hardlinked into the cache. Future
// calls for the same file will hardlink them from the cache to the
// target location. This reduces the amount of network traffic needed.
func NewHardlinkingFileFetcher(base FileFetcher, digestKeyFormat digest.KeyFormat, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64, evictionSet eviction.Set) FileFetcher {
	return &hardlinkingFileFetcher{
		base:            base,
		digestKeyFormat: digestKeyFormat,
		cacheDirectory:  cacheDirectory,
		maxFiles:        maxFiles,
		maxSize:         maxSize,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,
	}
}

func (ff *hardlinkingFileFetcher) makeSpace(size int64) error {
	for len(ff.filesSize) > 0 && (len(ff.filesSize) >= ff.maxFiles || ff.filesTotalSize+size > ff.maxSize) {
		// Remove a file from disk.
		key := ff.evictionSet.Peek()
		if err := ff.cacheDirectory.Remove(key); err != nil {
			return err
		}

		// Remove file from bookkeeping.
		ff.evictionSet.Remove()
		ff.filesTotalSize -= ff.filesSize[key]
		delete(ff.filesSize, key)
	}
	return nil
}

func (ff *hardlinkingFileFetcher) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	key := digest.GetKey(ff.digestKeyFormat)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	// If the file is present in the cache, hardlink it to the destination.
	ff.filesLock.RLock()
	if _, ok := ff.filesSize[key]; ok {
		ff.evictionLock.Lock()
		ff.evictionSet.Touch(key)
		ff.evictionLock.Unlock()

		err := ff.cacheDirectory.Link(key, directory, name)
		ff.filesLock.RUnlock()
		return err
	}
	ff.filesLock.RUnlock()

	// Download the file at the intended location.
	if err := ff.base.GetFile(ctx, digest, directory, name, isExecutable); err != nil {
		return err
	}

	ff.filesLock.Lock()
	defer ff.filesLock.Unlock()
	if _, ok := ff.filesSize[key]; !ok {
		ff.evictionLock.Lock()
		defer ff.evictionLock.Unlock()

		// Remove old files from the cache if necessary.
		sizeBytes := digest.GetSizeBytes()
		if err := ff.makeSpace(sizeBytes); err != nil {
			return err
		}

		// Hardlink the file into the cache.
		if err := directory.Link(name, ff.cacheDirectory, key); err != nil {
			return err
		}
		ff.evictionSet.Insert(key)
		ff.filesSize[key] = sizeBytes
		ff.filesTotalSize += sizeBytes
	}
	return nil
}
