package cas

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"

	"google.golang.org/protobuf/proto"
)

// CachingDirectoryFetcherKey is the key type that is used by
// CachingDirectoryFetcher.
//
// A separate boolean field is used to distinguish Directory objects
// that are roots of Tree objects from the others. This is needed
// because the digests of these objects don't match with that of the
// resulting Directory object, while for the others it does. Using the
// same key space for all objects collectively would be insecure.
type CachingDirectoryFetcherKey struct {
	DigestKey  string
	IsTreeRoot bool
}

// CachingDirectoryFetcherEvictionSet is the eviction set type that is
// accepted by NewCachingDirectoryFetcher().
type CachingDirectoryFetcherEvictionSet = eviction.Set[CachingDirectoryFetcherKey]

type cachingFetcherObject struct {
	directory *remoteexecution.Directory
	sizeBytes int64
}

type cachingDirectoryFetcher struct {
	base             DirectoryFetcher
	digestKeyFormat  digest.KeyFormat
	maximumCount     int
	maximumSizeBytes int64

	lock             sync.Mutex
	objects          map[CachingDirectoryFetcherKey]cachingFetcherObject
	objectsSizeBytes int64
	evictionSet      CachingDirectoryFetcherEvictionSet
}

// NewCachingDirectoryFetcher creates an adapter for DirectoryFetcher
// that caches up a fixed number of unmarshalled objects in memory. This
// reduces the amount of time spent unmarshaling messages, and may
// reduce the amount of network traffic generated.
func NewCachingDirectoryFetcher(base DirectoryFetcher, digestKeyFormat digest.KeyFormat, maximumCount int, maximumSizeBytes int64, evictionSet CachingDirectoryFetcherEvictionSet) DirectoryFetcher {
	return &cachingDirectoryFetcher{
		base:             base,
		digestKeyFormat:  digestKeyFormat,
		maximumCount:     maximumCount,
		maximumSizeBytes: maximumSizeBytes,

		objects:     map[CachingDirectoryFetcherKey]cachingFetcherObject{},
		evictionSet: evictionSet,
	}
}

func (df *cachingDirectoryFetcher) lookup(key CachingDirectoryFetcherKey) (*remoteexecution.Directory, bool) {
	df.lock.Lock()
	defer df.lock.Unlock()

	if object, ok := df.objects[key]; ok {
		df.evictionSet.Touch(key)
		return object.directory, true
	}
	return nil, false
}

func (df *cachingDirectoryFetcher) insert(key CachingDirectoryFetcherKey, directory *remoteexecution.Directory, sizeBytes int64) {
	df.lock.Lock()
	defer df.lock.Unlock()

	if _, ok := df.objects[key]; !ok {
		// Make space if needed.
		for len(df.objects) > 0 && (len(df.objects) >= df.maximumCount || df.objectsSizeBytes+sizeBytes > df.maximumSizeBytes) {
			key := df.evictionSet.Peek()
			df.evictionSet.Remove()
			df.objectsSizeBytes -= df.objects[key].sizeBytes
			delete(df.objects, key)
		}

		df.evictionSet.Insert(key)
		df.objects[key] = cachingFetcherObject{
			directory: directory,
			sizeBytes: sizeBytes,
		}
		df.objectsSizeBytes += sizeBytes
	}
}

func (df *cachingDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	key := CachingDirectoryFetcherKey{
		DigestKey:  directoryDigest.GetKey(df.digestKeyFormat),
		IsTreeRoot: false,
	}
	if directory, ok := df.lookup(key); ok {
		return directory, nil
	}

	directory, err := df.base.GetDirectory(ctx, directoryDigest)
	if err != nil {
		return nil, err
	}

	df.insert(key, directory, directoryDigest.GetSizeBytes())
	return directory, nil
}

func (df *cachingDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	key := CachingDirectoryFetcherKey{
		DigestKey:  treeDigest.GetKey(df.digestKeyFormat),
		IsTreeRoot: true,
	}
	if directory, ok := df.lookup(key); ok {
		return directory, nil
	}

	directory, err := df.base.GetTreeRootDirectory(ctx, treeDigest)
	if err != nil {
		return nil, err
	}

	// For this method the size of the resulting Directory message
	// is not known up front. Use proto.Size() to explicitly compute
	// the size of the cached entry.
	df.insert(key, directory, int64(proto.Size(directory)))
	return directory, nil
}

func (df *cachingDirectoryFetcher) GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error) {
	key := CachingDirectoryFetcherKey{
		DigestKey:  childDigest.GetKey(df.digestKeyFormat),
		IsTreeRoot: false,
	}
	if directory, ok := df.lookup(key); ok {
		return directory, nil
	}

	directory, err := df.base.GetTreeChildDirectory(ctx, treeDigest, childDigest)
	if err != nil {
		return nil, err
	}

	df.insert(key, directory, childDigest.GetSizeBytes())
	return directory, nil
}
