package cas

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type directoryCachingContentAddressableStorage struct {
	cas.ContentAddressableStorage

	digestKeyFormat util.DigestKeyFormat
	maxDirectories  int

	lock        sync.Mutex
	directories map[string]*remoteexecution.Directory
	evictionSet eviction.Set
}

// NewDirectoryCachingContentAddressableStorage is an adapter for
// ContentAddressableStorage that caches up a fixed number of
// unmarshalled directory objects in memory. This reduces the amount of
// network traffic needed.
func NewDirectoryCachingContentAddressableStorage(base cas.ContentAddressableStorage, digestKeyFormat util.DigestKeyFormat, maxDirectories int, evictionSet eviction.Set) cas.ContentAddressableStorage {
	return &directoryCachingContentAddressableStorage{
		ContentAddressableStorage: base,

		digestKeyFormat: digestKeyFormat,
		maxDirectories:  maxDirectories,

		directories: map[string]*remoteexecution.Directory{},
		evictionSet: evictionSet,
	}
}

func (cas *directoryCachingContentAddressableStorage) makeSpace() {
	for len(cas.directories) >= cas.maxDirectories {
		// Remove a directory from bookkeeping.
		key := cas.evictionSet.Peek()
		cas.evictionSet.Remove()
		delete(cas.directories, key)
	}
}

func (cas *directoryCachingContentAddressableStorage) GetDirectory(ctx context.Context, digest *util.Digest) (*remoteexecution.Directory, error) {
	key := digest.GetKey(cas.digestKeyFormat)

	// Check the cache.
	cas.lock.Lock()
	if directory, ok := cas.directories[key]; ok {
		cas.evictionSet.Touch(key)
		cas.lock.Unlock()
		return directory, nil
	}
	cas.lock.Unlock()

	// Not found. Download directory.
	directory, err := cas.ContentAddressableStorage.GetDirectory(ctx, digest)
	if err != nil {
		return nil, err
	}

	// Insert it into the cache.
	cas.lock.Lock()
	if _, ok := cas.directories[key]; !ok {
		cas.makeSpace()
		cas.evictionSet.Insert(key)
		cas.directories[key] = directory
	}
	cas.lock.Unlock()
	return directory, nil
}
