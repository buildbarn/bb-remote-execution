package blobstore

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type pendingPutOperation struct {
	digest digest.Digest
	b      buffer.Buffer
}

type batchedStoreBlobAccess struct {
	blobstore.BlobAccess
	blobKeyFormat digest.KeyFormat
	batchSize     int
	putSemaphore  *semaphore.Weighted

	lock                 sync.Mutex
	pendingPutOperations map[string]pendingPutOperation
	flushError           error
}

// NewBatchedStoreBlobAccess is an adapter for BlobAccess that causes
// Put() operations to be enqueued. When a sufficient number of
// operations are enqueued, a FindMissing() call is generated to
// determine which blobs actually need to be stored. Writes for blobs
// with the same digest are merged.
//
// This adapter may be used by the worker to speed up the uploading
// phase of actions.
func NewBatchedStoreBlobAccess(blobAccess blobstore.BlobAccess, blobKeyFormat digest.KeyFormat, batchSize int, putSemaphore *semaphore.Weighted) (blobstore.BlobAccess, func(ctx context.Context) error) {
	ba := &batchedStoreBlobAccess{
		BlobAccess:           blobAccess,
		blobKeyFormat:        blobKeyFormat,
		batchSize:            batchSize,
		pendingPutOperations: map[string]pendingPutOperation{},
		putSemaphore:         putSemaphore,
	}
	return ba, func(ctx context.Context) error {
		ba.lock.Lock()
		defer ba.lock.Unlock()

		// Flush last batch of blobs. Return any errors that occurred.
		ba.flushLocked(ctx)
		err := ba.flushError
		ba.flushError = nil
		return err
	}
}

func (ba *batchedStoreBlobAccess) flushLocked(ctx context.Context) {
	// Ensure that all pending blobs are closed upon termination.
	defer func() {
		for _, pendingPutOperation := range ba.pendingPutOperations {
			pendingPutOperation.b.Discard()
		}
		ba.pendingPutOperations = map[string]pendingPutOperation{}
	}()

	// Determine which blobs are missing.
	digests := digest.NewSetBuilder()
	for _, pendingPutOperation := range ba.pendingPutOperations {
		digests.Add(pendingPutOperation.digest)
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, digests.Build())
	if err != nil {
		ba.flushError = util.StatusWrap(err, "Failed to determine existence of previous batch of blobs")
		return
	}

	// Upload the missing ones.
	group, groupCtx := errgroup.WithContext(ctx)
	for _, digest := range missing.Items() {
		key := digest.GetKey(ba.blobKeyFormat)
		if pendingPutOperation, ok := ba.pendingPutOperations[key]; ok {
			if groupCtx.Err() != nil || ba.putSemaphore.Acquire(groupCtx, 1) != nil {
				break
			}
			delete(ba.pendingPutOperations, key)
			group.Go(func() error {
				err := ba.BlobAccess.Put(groupCtx, pendingPutOperation.digest, pendingPutOperation.b)
				ba.putSemaphore.Release(1)
				if err != nil {
					return util.StatusWrapf(err, "Failed to store previous blob %s", pendingPutOperation.digest)
				}
				return nil
			})
		}
	}
	if err := group.Wait(); err != nil {
		ba.flushError = err
	} else if err := util.StatusFromContext(ctx); err != nil {
		ba.flushError = err
	}
}

func (ba *batchedStoreBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	// Discard duplicate writes.
	key := digest.GetKey(ba.blobKeyFormat)
	if _, ok := ba.pendingPutOperations[key]; ok {
		b.Discard()
		return nil
	}

	// Flush the existing blobs if there are too many pending.
	if len(ba.pendingPutOperations) >= ba.batchSize {
		ba.flushLocked(ctx)
	}
	if err := ba.flushError; err != nil {
		b.Discard()
		return err
	}

	ba.pendingPutOperations[key] = pendingPutOperation{
		digest: digest,
		b:      b,
	}
	return nil
}
