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
	evictionSet  eviction.Set
}

// NewHardlinkingFileFetcher is an adapter for FileFetcher that stores
// files in an internal directory. After successfully downloading files
// at the target location, they are hardlinked into the cache. Future
// calls for the same file will hardlink them from the cache to the
// target location. This reduces the amount of network traffic needed.
func NewHardlinkingFileFetcher(base FileFetcher, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64, evictionSet eviction.Set) FileFetcher {
	return &hardlinkingFileFetcher{
		base:           base,
		cacheDirectory: cacheDirectory,
		maxFiles:       maxFiles,
		maxSize:        maxSize,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,
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

	// If the file is present in the cache, hardlink it to the destination.
	wasMissing := false
	ff.filesLock.RLock()
	if _, ok := ff.filesSize[key]; ok {
		ff.evictionLock.Lock()
		ff.evictionSet.Touch(key)
		ff.evictionLock.Unlock()

		if err := ff.cacheDirectory.Link(path.MustNewComponent(key), directory, name); err == nil {
			// Successfully hardlinked the file to its destination.
			ff.filesLock.RUnlock()
			return nil
		} else if !os.IsNotExist(err) {
			ff.filesLock.RUnlock()
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create hardlink to cached file %#v", key)
		}

		// The file was part of the cache, even though it did not
		// exist on disk. Some other process may have tampered
		// with the cache directory's contents.
		wasMissing = true
	}
	ff.filesLock.RUnlock()

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

		// Hardlink the file into the cache.
		if err := directory.Link(name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to add cached file %#v", key)
		}
		ff.evictionSet.Insert(key)
		ff.filesSize[key] = sizeBytes
		ff.filesTotalSize += sizeBytes
	} else if wasMissing {
		// Even though the file is part of our bookkeeping, we
		// observed it didn't exist. Repair this inconsistency.
		if err := directory.Link(name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to repair cached file %#v", key)
		}
	}
	return nil
}
