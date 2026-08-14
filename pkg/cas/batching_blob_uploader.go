package cas

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type pendingUploadOperation struct {
	digest digest.Digest
	blob   Blob
}

type batchingBlobUploader struct {
	contentAddressableStorage  cdc.ContentAddressableStorage
	digestKeyFormat            digest.KeyFormat
	batchSize                  int
	uploadConcurrencySemaphore *semaphore.Weighted

	lock                    sync.Mutex
	pendingUploadOperations map[string]pendingUploadOperation
	flushError              error
}

// NewBatchingBlobUploader returns a BlobUploader that batches uploads
// to the Content Addressable Storage (CAS) into batches of the
// specified size while still respecting an upload concurrency.
func NewBatchingBlobUploader(contentAddressableStorage cdc.ContentAddressableStorage, batchSize int, uploadConcurrencySemaphore *semaphore.Weighted) (BlobUploader, func(context.Context) error) {
	bu := &batchingBlobUploader{
		contentAddressableStorage:  contentAddressableStorage,
		digestKeyFormat:            contentAddressableStorage.GetDigestKeyFormat(),
		batchSize:                  batchSize,
		uploadConcurrencySemaphore: uploadConcurrencySemaphore,
		pendingUploadOperations:    map[string]pendingUploadOperation{},
	}
	return bu, func(ctx context.Context) error {
		bu.lock.Lock()
		defer bu.lock.Unlock()

		// Flush last batch of blobs. Return any errors that occurred.
		bu.flushLocked(ctx)
		err := bu.flushError
		bu.flushError = nil
		return err
	}
}

func (bu *batchingBlobUploader) flushLocked(ctx context.Context) {
	// Ensure that all pending blobs are closed upon termination.
	defer func() {
		for _, pending := range bu.pendingUploadOperations {
			pending.blob.Discard()
		}
		bu.pendingUploadOperations = map[string]pendingUploadOperation{}
	}()

	if len(bu.pendingUploadOperations) == 0 {
		return
	}

	// Determine which blobs are missing.
	digests := digest.NewSetBuilder(len(bu.pendingUploadOperations))
	for _, pending := range bu.pendingUploadOperations {
		digests.Add(pending.digest)
	}

	missing, err := bu.contentAddressableStorage.FindMissing(ctx, digests.Build())
	if err != nil {
		bu.flushError = util.StatusWrap(err, "Failed to determine existence of previous batch of blobs")
		return
	}

	// Upload the missing ones.
	if !missing.Empty() {
		group, groupCtx := errgroup.WithContext(ctx)
		group.Go(func() error {
			for _, d := range missing.Items() {
				key := d.GetKey(bu.digestKeyFormat)
				if pending, ok := bu.pendingUploadOperations[key]; ok {
					// Mirroring batchedStoreBlobAccess: Acquire semaphore before spinning up the goroutine.
					if err := util.AcquireSemaphore(groupCtx, bu.uploadConcurrencySemaphore, 1); err != nil {
						return err
					}
					delete(bu.pendingUploadOperations, key)
					group.Go(func() error {
						err := PutBlob(groupCtx, bu.contentAddressableStorage, pending.digest, pending.blob)
						bu.uploadConcurrencySemaphore.Release(1)
						if err != nil {
							return util.StatusWrapf(err, "Failed to store previous blob %s", pending.digest)
						}
						return nil
					})
				}
			}
			return nil
		})
		if err := group.Wait(); err != nil {
			bu.flushError = err
		}
	}
}

func (bu *batchingBlobUploader) UploadBlob(ctx context.Context, d digest.Digest, blob Blob) error {
	bu.lock.Lock()
	defer bu.lock.Unlock()

	// Discard duplicate writes.
	key := d.GetKey(bu.digestKeyFormat)
	if _, ok := bu.pendingUploadOperations[key]; ok {
		blob.Discard()
		return nil
	}

	// Flush the existing blobs if there are too many pending.
	if len(bu.pendingUploadOperations) >= bu.batchSize {
		bu.flushLocked(ctx)
	}
	if err := bu.flushError; err != nil {
		blob.Discard()
		return err
	}

	bu.pendingUploadOperations[key] = pendingUploadOperation{
		digest: d,
		blob:   blob,
	}
	return nil
}
