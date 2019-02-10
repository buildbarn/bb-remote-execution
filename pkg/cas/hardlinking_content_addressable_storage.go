package cas

import (
	"context"
	"math/rand"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	hardlinkingContentAddressableStorageOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "cas",
			Name:      "hardlinking_content_addressable_storage_operations_total",
			Help:      "Total number of operations against the hardlinking content addressable storage.",
		},
		[]string{"result"})
	hardlinkingContentAddressableStorageOperationsTotalHit  = hardlinkingContentAddressableStorageOperationsTotal.WithLabelValues("Hit")
	hardlinkingContentAddressableStorageOperationsTotalMiss = hardlinkingContentAddressableStorageOperationsTotal.WithLabelValues("Miss")
)

func init() {
	prometheus.MustRegister(hardlinkingContentAddressableStorageOperationsTotal)
}

type hardlinkingContentAddressableStorage struct {
	cas.ContentAddressableStorage

	lock sync.RWMutex

	digestKeyFormat util.DigestKeyFormat
	cacheDirectory  filesystem.Directory
	maxFiles        int
	maxSize         int64

	filesPresentList      []string
	filesPresentSize      map[string]int64
	filesPresentTotalSize int64
}

// NewHardlinkingContentAddressableStorage is an adapter for
// ContentAddressableStorage that stores files in an internal directory. After
// successfully downloading files at the target location, they are hardlinked
// into the cache. Future calls for the same file will hardlink them from the
// cache to the target location. This reduces the amount of network traffic
// needed.
func NewHardlinkingContentAddressableStorage(base cas.ContentAddressableStorage, digestKeyFormat util.DigestKeyFormat, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64) cas.ContentAddressableStorage {
	return &hardlinkingContentAddressableStorage{
		ContentAddressableStorage: base,

		digestKeyFormat: digestKeyFormat,
		cacheDirectory:  cacheDirectory,
		maxFiles:        maxFiles,
		maxSize:         maxSize,

		filesPresentSize: map[string]int64{},
	}
}

func (cas *hardlinkingContentAddressableStorage) makeSpace(size int64) error {
	for len(cas.filesPresentList) > 0 && (len(cas.filesPresentList) >= cas.maxFiles || cas.filesPresentTotalSize+size > cas.maxSize) {
		// Remove random file from disk.
		idx := rand.Intn(len(cas.filesPresentList))
		key := cas.filesPresentList[idx]
		if err := cas.cacheDirectory.Remove(key); err != nil {
			return err
		}

		// Remove file from bookkeeping.
		cas.filesPresentTotalSize -= cas.filesPresentSize[key]
		delete(cas.filesPresentSize, key)
		last := len(cas.filesPresentList) - 1
		cas.filesPresentList[idx] = cas.filesPresentList[last]
		cas.filesPresentList = cas.filesPresentList[:last]
	}
	return nil
}

func (cas *hardlinkingContentAddressableStorage) GetFile(ctx context.Context, digest *util.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	key := digest.GetKey(cas.digestKeyFormat)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	// If the file is present in the cache, hardlink it to the destination.
	cas.lock.RLock()
	if _, ok := cas.filesPresentSize[key]; ok {
		err := cas.cacheDirectory.Link(key, directory, name)
		cas.lock.RUnlock()
		hardlinkingContentAddressableStorageOperationsTotalHit.Inc()
		return err
	}
	cas.lock.RUnlock()
	hardlinkingContentAddressableStorageOperationsTotalMiss.Inc()

	// Download the file at the intended location.
	if err := cas.ContentAddressableStorage.GetFile(ctx, digest, directory, name, isExecutable); err != nil {
		return err
	}

	// Hardlink the file into the cache.
	cas.lock.Lock()
	defer cas.lock.Unlock()
	if _, ok := cas.filesPresentSize[key]; !ok {
		sizeBytes := digest.GetSizeBytes()
		if err := cas.makeSpace(sizeBytes); err != nil {
			return err
		}
		if err := directory.Link(name, cas.cacheDirectory, key); err != nil {
			return err
		}
		cas.filesPresentList = append(cas.filesPresentList, key)
		cas.filesPresentSize[key] = sizeBytes
		cas.filesPresentTotalSize += sizeBytes
	}
	return nil
}
