package cas

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
)

type cachingDirectoryFetcher struct {
	base            DirectoryFetcher
	digestKeyFormat digest.KeyFormat
	maxDirectories  int

	lock        sync.Mutex
	directories map[string]*remoteexecution.Directory
	evictionSet eviction.Set
}

// NewCachingDirectoryFetcher is an adapter for DirectoryFetcher that
// caches up a fixed number of unmarshalled directory objects in memory.
// This reduces the amount of network traffic needed.
func NewCachingDirectoryFetcher(base DirectoryFetcher, digestKeyFormat digest.KeyFormat, maxDirectories int, evictionSet eviction.Set) DirectoryFetcher {
	return &cachingDirectoryFetcher{
		base:            base,
		digestKeyFormat: digestKeyFormat,
		maxDirectories:  maxDirectories,

		directories: map[string]*remoteexecution.Directory{},
		evictionSet: evictionSet,
	}
}

func (df *cachingDirectoryFetcher) makeSpace() {
	for len(df.directories) >= df.maxDirectories {
		// Remove a directory from bookkeeping.
		key := df.evictionSet.Peek()
		df.evictionSet.Remove()
		delete(df.directories, key)
	}
}

func (df *cachingDirectoryFetcher) GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	key := digest.GetKey(df.digestKeyFormat)

	// Check the cache.
	df.lock.Lock()
	if directory, ok := df.directories[key]; ok {
		df.evictionSet.Touch(key)
		df.lock.Unlock()
		return directory, nil
	}
	df.lock.Unlock()

	// Not found. Download directory.
	directory, err := df.base.GetDirectory(ctx, digest)
	if err != nil {
		return nil, err
	}

	// Insert it into the cache.
	df.lock.Lock()
	if _, ok := df.directories[key]; !ok {
		df.makeSpace()
		df.evictionSet.Insert(key)
		df.directories[key] = directory
	}
	df.lock.Unlock()
	return directory, nil
}
