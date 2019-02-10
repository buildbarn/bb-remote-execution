package cas

import (
	"context"
	"math/rand"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type directoryCachingContentAddressableStorage struct {
	cas.ContentAddressableStorage

	lock sync.RWMutex

	digestKeyFormat util.DigestKeyFormat
	maxDirectories  int

	directoriesPresentList    []string
	directoriesPresentMessage map[string]*remoteexecution.Directory
}

// NewDirectoryCachingContentAddressableStorage is an adapter for
// ContentAddressableStorage that caches up a fixed number of
// unmarshalled directory objects in memory. This reduces the amount of
// network traffic needed.
func NewDirectoryCachingContentAddressableStorage(base cas.ContentAddressableStorage, digestKeyFormat util.DigestKeyFormat, maxDirectories int) cas.ContentAddressableStorage {
	return &directoryCachingContentAddressableStorage{
		ContentAddressableStorage: base,

		digestKeyFormat: digestKeyFormat,
		maxDirectories:  maxDirectories,

		directoriesPresentMessage: map[string]*remoteexecution.Directory{},
	}
}

func (cas *directoryCachingContentAddressableStorage) makeSpace() {
	for len(cas.directoriesPresentList) >= cas.maxDirectories {
		// Pick random directory to remove.
		idx := rand.Intn(len(cas.directoriesPresentList))
		key := cas.directoriesPresentList[idx]

		// Remove directory from bookkeeping.
		delete(cas.directoriesPresentMessage, key)
		last := len(cas.directoriesPresentList) - 1
		cas.directoriesPresentList[idx] = cas.directoriesPresentList[last]
		cas.directoriesPresentList = cas.directoriesPresentList[:last]
	}
}

func (cas *directoryCachingContentAddressableStorage) GetDirectory(ctx context.Context, digest *util.Digest) (*remoteexecution.Directory, error) {
	key := digest.GetKey(cas.digestKeyFormat)

	// Check the cache.
	cas.lock.RLock()
	directory, ok := cas.directoriesPresentMessage[key]
	cas.lock.RUnlock()
	if ok {
		return directory, nil
	}

	// Not found. Download directory.
	directory, err := cas.ContentAddressableStorage.GetDirectory(ctx, digest)
	if err != nil {
		return nil, err
	}

	// Insert it into the cache.
	cas.lock.Lock()
	if _, ok := cas.directoriesPresentMessage[key]; !ok {
		cas.makeSpace()
		cas.directoriesPresentList = append(cas.directoriesPresentList, key)
		cas.directoriesPresentMessage[key] = directory
	}
	cas.lock.Unlock()
	return directory, nil
}
