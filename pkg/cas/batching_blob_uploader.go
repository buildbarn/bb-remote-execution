package cas

import (
	"context"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type pendingUploadOperation struct {
	digest digest.Digest
	file   buffer.ReadAtCloser
}

type batchingBlobUploader struct {
	contentAddressableStorage  cas.ContentAddressableStorage
	digestKeyFormat            digest.KeyFormat
	batchSize                  int
	uploadConcurrencySemaphore *semaphore.Weighted

	lock                    sync.Mutex
	pendingUploadOperations map[string]pendingUploadOperation
	flushError              error
}

// NewBatchingBlobUploader returns a BlobUploader that batches uploads
// to the Content Addressable Storage (CAS) into batches of the
// specified size while respecting an upload concurrency.
func NewBatchingBlobUploader(contentAddressableStorage cas.ContentAddressableStorage, batchSize int, uploadConcurrencySemaphore *semaphore.Weighted) (BlobUploader, func(context.Context) error) {
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
			pending.file.Close()
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
					if err := util.AcquireSemaphore(groupCtx, bu.uploadConcurrencySemaphore, 1); err != nil {
						return err
					}
					delete(bu.pendingUploadOperations, key)
					group.Go(func() error {
						defer pending.file.Close()
						// TODO: Use our random access io to do
						// multithreaded chunking.
						err := cas.PutReader(groupCtx, bu.contentAddressableStorage, pending.digest, io.NewSectionReader(pending.file, 0, d.GetSizeBytes()))
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

func (bu *batchingBlobUploader) uploadBlob(ctx context.Context, d digest.Digest, blob buffer.ReadAtCloser) error {
	bu.lock.Lock()
	defer bu.lock.Unlock()

	// Discard duplicate writes.
	key := d.GetKey(bu.digestKeyFormat)
	if _, ok := bu.pendingUploadOperations[key]; ok {
		blob.Close()
		return nil
	}

	// Flush the existing blobs if there are too many pending.
	if len(bu.pendingUploadOperations) >= bu.batchSize {
		bu.flushLocked(ctx)
	}
	if err := bu.flushError; err != nil {
		blob.Close()
		return err
	}

	bu.pendingUploadOperations[key] = pendingUploadOperation{
		digest: d,
		file:   blob,
	}
	return nil
}

func (bu *batchingBlobUploader) UploadBlob(ctx context.Context, digestFunction digest.Function, blob filesystem.FileReader) (digest.Digest, error) {
	sizeBytes, err := blob.Len()
	if err != nil {
		return digest.BadDigest, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := digestFunction.NewGenerator(sizeBytes)
	if _, err := io.Copy(digestGenerator, io.NewSectionReader(blob, 0, sizeBytes)); err != nil {
		blob.Close()
		return digest.BadDigest, util.StatusWrap(err, "Failed to compute file digest")
	}
	blobDigest := digestGenerator.Sum()

	// Rewind and store it. Limit uploading to the size that was
	// used to compute the digest. This ensures uploads succeed,
	// even if more data gets appended in the meantime. This is not
	// uncommon, especially for stdout and stderr logs.
	if err := bu.uploadBlob(
		ctx,
		blobDigest,
		newSectionReadAtCloser(blob, 0, sizeBytes),
	); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}

// newSectionReadAtCloser returns a buffer.ReadAtCloser that reads from
// r at a given offset, but stops with EOF after n bytes. This function
// is identical to io.NewSectionReader(), except that it provides an
// buffer.ReadAtCloser instead of an io.ReaderAt.
func newSectionReadAtCloser(r filesystem.FileReader, off, n int64) buffer.ReadAtCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, off, n),
		Closer:        r,
	}
}
