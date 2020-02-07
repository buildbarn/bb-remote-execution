package blobstore

import (
	"context"
	"sort"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type pendingPutOperation struct {
	digest digest.Digest
	b      buffer.Buffer
}

type batchedStoreBlobAccess struct {
	blobstore.BlobAccess
	blobKeyFormat digest.KeyFormat
	batchSize     int

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
func NewBatchedStoreBlobAccess(blobAccess blobstore.BlobAccess, blobKeyFormat digest.KeyFormat, batchSize int) (blobstore.BlobAccess, func(ctx context.Context) error) {
	ba := &batchedStoreBlobAccess{
		BlobAccess:           blobAccess,
		blobKeyFormat:        blobKeyFormat,
		batchSize:            batchSize,
		pendingPutOperations: map[string]pendingPutOperation{},
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

	// Determine which blobs are missing. Pass blobs to
	// FindMissing() in sorted order for testability.
	keys := make([]string, 0, len(ba.pendingPutOperations))
	for key := range ba.pendingPutOperations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	digests := digest.NewSetBuilder()
	for _, key := range keys {
		digests.Add(ba.pendingPutOperations[key].digest)
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, digests.Build())
	if err != nil {
		ba.flushError = util.StatusWrap(err, "Failed to determine existence of previous batch of blobs")
		return
	}

	// Upload the missing ones.
	for _, digest := range missing.Items() {
		key := digest.GetKey(ba.blobKeyFormat)
		if pendingPutOperation, ok := ba.pendingPutOperations[key]; ok {
			delete(ba.pendingPutOperations, key)
			if err := ba.BlobAccess.Put(ctx, pendingPutOperation.digest, pendingPutOperation.b); err != nil {
				ba.flushError = util.StatusWrapf(err, "Failed to store previous blob %s", pendingPutOperation.digest)
				return
			}
		}
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
