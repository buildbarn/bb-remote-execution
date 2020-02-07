package cas

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type hardlinkingContentAddressableStorage struct {
	cas.ContentAddressableStorage

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

// NewHardlinkingContentAddressableStorage is an adapter for
// ContentAddressableStorage that stores files in an internal directory. After
// successfully downloading files at the target location, they are hardlinked
// into the cache. Future calls for the same file will hardlink them from the
// cache to the target location. This reduces the amount of network traffic
// needed.
func NewHardlinkingContentAddressableStorage(base cas.ContentAddressableStorage, digestKeyFormat digest.KeyFormat, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64, evictionSet eviction.Set) cas.ContentAddressableStorage {
	return &hardlinkingContentAddressableStorage{
		ContentAddressableStorage: base,

		digestKeyFormat: digestKeyFormat,
		cacheDirectory:  cacheDirectory,
		maxFiles:        maxFiles,
		maxSize:         maxSize,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,
	}
}

func (cas *hardlinkingContentAddressableStorage) makeSpace(size int64) error {
	for len(cas.filesSize) > 0 && (len(cas.filesSize) >= cas.maxFiles || cas.filesTotalSize+size > cas.maxSize) {
		// Remove a file from disk.
		key := cas.evictionSet.Peek()
		if err := cas.cacheDirectory.Remove(key); err != nil {
			return err
		}

		// Remove file from bookkeeping.
		cas.evictionSet.Remove()
		cas.filesTotalSize -= cas.filesSize[key]
		delete(cas.filesSize, key)
	}
	return nil
}

func (cas *hardlinkingContentAddressableStorage) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	key := digest.GetKey(cas.digestKeyFormat)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	// If the file is present in the cache, hardlink it to the destination.
	cas.filesLock.RLock()
	if _, ok := cas.filesSize[key]; ok {
		cas.evictionLock.Lock()
		cas.evictionSet.Touch(key)
		cas.evictionLock.Unlock()

		err := cas.cacheDirectory.Link(key, directory, name)
		cas.filesLock.RUnlock()
		return err
	}
	cas.filesLock.RUnlock()

	// Download the file at the intended location.
	if err := cas.ContentAddressableStorage.GetFile(ctx, digest, directory, name, isExecutable); err != nil {
		return err
	}

	cas.filesLock.Lock()
	defer cas.filesLock.Unlock()
	if _, ok := cas.filesSize[key]; !ok {
		cas.evictionLock.Lock()
		defer cas.evictionLock.Unlock()

		// Remove old files from the cache if necessary.
		sizeBytes := digest.GetSizeBytes()
		if err := cas.makeSpace(sizeBytes); err != nil {
			return err
		}

		// Hardlink the file into the cache.
		if err := directory.Link(name, cas.cacheDirectory, key); err != nil {
			return err
		}
		cas.evictionSet.Insert(key)
		cas.filesSize[key] = sizeBytes
		cas.filesTotalSize += sizeBytes
	}
	return nil
}
