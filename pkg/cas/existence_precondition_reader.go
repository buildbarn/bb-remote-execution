package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type existencePreconditionReader[T any] struct {
	base reader.Reader[T]
}

// NewExistencePreconditionReader wraps a Reader that reads blobs from
// the Content Addressable Storage (CAS) into one that reports missing
// blobs as FAILED_PRECONDITION, annotating them with a
// PreconditionFailure that identifies the blob as being missing. This
// is needed to make Execution::Execute() comply to the remote execution
// protocol: any blob needed by a build action that is not present in
// the CAS must be reported as such, so that the client can upload it
// again.
//
// This decorator should only be used on read sites that serve remote
// execution requests, such as actions read by the scheduler and
// commands/directories read by workers.
func NewExistencePreconditionReader[T any](base reader.Reader[T]) reader.Reader[T] {
	return &existencePreconditionReader[T]{
		base: base,
	}
}

func (r *existencePreconditionReader[T]) Read(ctx context.Context, d digest.Digest) (T, error) {
	value, err := r.base.Read(ctx, d)
	if err != nil {
		return value, FailedPreconditionOnMissingBlob(d, err)
	}
	return value, nil
}
