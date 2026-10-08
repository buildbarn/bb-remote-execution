package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type suspendingBlobAccess[T any] struct {
	blobstore.BlobAccess[T]
	suspendable clock.Suspendable
}

// NewSuspendingBlobAccess is a decorator for a BlobAccess that suspends
// a clock.Suspendable object for the duration of every operation.
//
// This decorator is used in combination with SuspendableClock, allowing
// VFS-based workers to compensate the execution timeout of build
// actions for any time spent accessing storage.
func NewSuspendingBlobAccess[T any](blobAccess blobstore.BlobAccess[T], suspendable clock.Suspendable) blobstore.BlobAccess[T] {
	return &suspendingBlobAccess[T]{
		BlobAccess:  blobAccess,
		suspendable: suspendable,
	}
}

func (ba suspendingBlobAccess[T]) Get(ctx context.Context, d digest.Digest) (T, error) {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()
	return ba.BlobAccess.Get(ctx, d)
}

func (ba suspendingBlobAccess[T]) Put(ctx context.Context, d digest.Digest, value T) error {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()
	return ba.BlobAccess.Put(ctx, d, value)
}

func (ba suspendingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()
	return ba.BlobAccess.FindMissing(ctx, digests)
}

type suspendingChunkBytesReader struct {
	reader.Reader[[]byte]
	suspendable clock.Suspendable
}

// NewSuspendingChunkBytesReader is a decorator for a reader.Reader of
// chunk bytes that suspends a clock.Suspendable object for the duration
// of every read.
func NewSuspendingChunkBytesReader(base reader.Reader[[]byte], suspendable clock.Suspendable) reader.Reader[[]byte] {
	return &suspendingChunkBytesReader{
		Reader:      base,
		suspendable: suspendable,
	}
}

func (r suspendingChunkBytesReader) Read(ctx context.Context, d digest.Digest) ([]byte, error) {
	r.suspendable.Suspend()
	defer r.suspendable.Resume()
	return r.Reader.Read(ctx, d)
}

type suspendingChunkMappingFetcher struct {
	chunk.MappingFetcher
	suspendable clock.Suspendable
}

// NewSuspendingChunkMappingFetcher is a decorator for a
// chunk.MappingFetcher that suspends a clock.Suspendable object for the
// duration of every operation.
func NewSuspendingChunkMappingFetcher(fetcher chunk.MappingFetcher, suspendable clock.Suspendable) chunk.MappingFetcher {
	return &suspendingChunkMappingFetcher{
		MappingFetcher: fetcher,
		suspendable:    suspendable,
	}
}

func (f suspendingChunkMappingFetcher) FetchChunkMapping(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	f.suspendable.Suspend()
	defer f.suspendable.Resume()
	return f.MappingFetcher.FetchChunkMapping(ctx, d)
}

type suspendingParametersFetcher struct {
	capabilities.CDCParametersFetcher
	suspendable clock.Suspendable
}

// NewSuspendingParametersFetcher is a decorator for a
// capabilities.CDCParametersFetcher that suspends a clock.Suspendable
// object for the duration of every operation.
func NewSuspendingParametersFetcher(fetcher capabilities.CDCParametersFetcher, suspendable clock.Suspendable) capabilities.CDCParametersFetcher {
	return &suspendingParametersFetcher{
		CDCParametersFetcher: fetcher,
		suspendable:          suspendable,
	}
}

func (f suspendingParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	f.suspendable.Suspend()
	defer f.suspendable.Resume()
	return f.CDCParametersFetcher.FetchCDCParameters(ctx, instanceName)
}
