package cas

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
)

type cachingFetcherObject[T any] struct {
	value     *T
	sizeBytes int64
}

type cachingFetcher[T any] struct {
	base             Fetcher[T]
	digestKeyFormat  digest.KeyFormat
	maximumCount     int
	maximumSizeBytes int64

	lock             sync.Mutex
	objects          map[string]cachingFetcherObject[T]
	objectsSizeBytes int64
	evictionSet      eviction.Set
}

// NewCachingFetcher is an adapter for Fetcher that caches up a fixed
// number of unmarshalled objects in memory. This reduces the amount of
// network traffic needed.
func NewCachingFetcher[T any](base Fetcher[T], digestKeyFormat digest.KeyFormat, maximumCount int, maximumSizeBytes int64, evictionSet eviction.Set) Fetcher[T] {
	return &cachingFetcher[T]{
		base:            base,
		digestKeyFormat: digestKeyFormat,
		maximumCount:    maximumCount,

		objects:     map[string]cachingFetcherObject[T]{},
		evictionSet: evictionSet,
	}
}

func (df *cachingFetcher[T]) makeSpace(sizeBytes int64) {
	for len(df.objects) > 0 && (len(df.objects) >= df.maximumCount || df.objectsSizeBytes+sizeBytes > df.maximumSizeBytes) {
		// Remove an object from bookkeeping.
		key := df.evictionSet.Peek()
		df.evictionSet.Remove()
		df.objectsSizeBytes -= df.objects[key].sizeBytes
		delete(df.objects, key)
	}
}

func (df *cachingFetcher[T]) Get(ctx context.Context, digest digest.Digest) (*T, error) {
	key := digest.GetKey(df.digestKeyFormat)

	// Check the cache.
	df.lock.Lock()
	if object, ok := df.objects[key]; ok {
		df.evictionSet.Touch(key)
		df.lock.Unlock()
		return object.value, nil
	}
	df.lock.Unlock()

	// Not found. Download object.
	value, err := df.base.Get(ctx, digest)
	if err != nil {
		return nil, err
	}

	// Insert it into the cache.
	df.lock.Lock()
	if _, ok := df.objects[key]; !ok {
		sizeBytes := digest.GetSizeBytes()
		df.makeSpace(sizeBytes)
		df.evictionSet.Insert(key)
		df.objects[key] = cachingFetcherObject[T]{
			value:     value,
			sizeBytes: sizeBytes,
		}
		df.objectsSizeBytes += sizeBytes
	}
	df.lock.Unlock()
	return value, nil
}
