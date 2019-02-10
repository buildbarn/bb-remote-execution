package blobstore

import (
	"context"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type pendingPutOperation struct {
	digest    *util.Digest
	sizeBytes int64
	r         io.ReadCloser
}

type batchedStoreBlobAccess struct {
	blobstore.BlobAccess
	blobKeyFormat util.DigestKeyFormat
	batchSize     int

	lock                 sync.Mutex
	pendingPutOperations map[string]pendingPutOperation
}

// NewBatchedStoreBlobAccess is an adapter for BlobAccess that causes
// Put() operations to be enqueued. When a sufficient number of
// operations are enqueued, a FindMissing() call is generated to
// determine which blobs actually need to be stored. Writes for blobs
// with the same digest are merged.
//
// This adapter may be used by the worker to speed up the uploading
// phase of actions.
func NewBatchedStoreBlobAccess(blobAccess blobstore.BlobAccess, blobKeyFormat util.DigestKeyFormat, batchSize int) (blobstore.BlobAccess, func(ctx context.Context) error) {
	ba := &batchedStoreBlobAccess{
		BlobAccess:           blobAccess,
		blobKeyFormat:        blobKeyFormat,
		batchSize:            batchSize,
		pendingPutOperations: map[string]pendingPutOperation{},
	}
	return ba, func(ctx context.Context) error {
		ba.lock.Lock()
		defer ba.lock.Unlock()
		return ba.flushLocked(ctx)
	}
}

func (ba *batchedStoreBlobAccess) flushLocked(ctx context.Context) error {
	// Determine which blobs are missing.
	var digests []*util.Digest
	for _, pendingPutOperation := range ba.pendingPutOperations {
		digests = append(digests, pendingPutOperation.digest)
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return err
	}

	// Upload the missing ones.
	for _, digest := range missing {
		key := digest.GetKey(ba.blobKeyFormat)
		if pendingPutOperation, ok := ba.pendingPutOperations[key]; ok {
			delete(ba.pendingPutOperations, key)
			if err := ba.BlobAccess.Put(ctx, pendingPutOperation.digest, pendingPutOperation.sizeBytes, pendingPutOperation.r); err != nil {
				return err
			}
		}
	}

	// Discard the others.
	for _, pendingPutOperation := range ba.pendingPutOperations {
		pendingPutOperation.r.Close()
	}
	ba.pendingPutOperations = map[string]pendingPutOperation{}
	return nil
}

func (ba *batchedStoreBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	// First flush the existing files if there are too many pending.
	ba.lock.Lock()
	defer ba.lock.Unlock()
	if len(ba.pendingPutOperations) >= ba.batchSize {
		if err := ba.flushLocked(ctx); err != nil {
			r.Close()
			return err
		}
	}

	// Discard duplicate writes.
	key := digest.GetKey(ba.blobKeyFormat)
	if _, ok := ba.pendingPutOperations[key]; ok {
		return r.Close()
	}

	ba.pendingPutOperations[key] = pendingPutOperation{
		digest:    digest,
		sizeBytes: sizeBytes,
		r:         r,
	}
	return nil
}
