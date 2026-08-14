package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type existencePreconditionStreamReader struct {
	base cas.StreamReader
}

// NewExistencePreconditionStreamReader wraps a StreamReader that reads
// blobs from the Content Addressable Storage (CAS) into one that
// reports missing blobs as FAILED_PRECONDITION, annotating them with a
// PreconditionFailure that identifies the blob as being missing. This
// is needed to make Execution::Execute() comply to the remote
// execution protocol: any blob needed by a build action that is not
// present in the CAS must be reported as such, so that the client can
// upload it again.
//
// This decorator should only be used on read sites that serve remote
// execution requests, such as trees read by workers. Servers that
// expose the CAS to clients directly (e.g., HTTP file serving) should
// report missing blobs as NOT_FOUND.
//
// Because blobs may be chunked, a blob may only be found to be missing
// while reading from the stream that is returned by ReadStream(). The
// stream returned by this decorator therefore rewrites any NOT_FOUND
// errors that are observed into FAILED_PRECONDITION errors that refer
// to the blob, as opposed to one of its chunks.
func NewExistencePreconditionStreamReader(base cas.StreamReader) cas.StreamReader {
	return &existencePreconditionStreamReader{
		base: base,
	}
}

func (r *existencePreconditionStreamReader) ReadStream(ctx context.Context, d digest.Digest) (io.Reader, error) {
	s, err := r.base.ReadStream(ctx, d)
	if err != nil {
		return nil, FailedPreconditionOnMissingBlob(d, err)
	}
	return &existencePreconditionStream{
		base:       s,
		blobDigest: d,
	}, nil
}

// existencePreconditionStream is an io.Reader that rewrites NOT_FOUND
// errors that are observed while reading from a blob into
// FAILED_PRECONDITION errors that refer to the blob, as opposed to one
// of its chunks.
type existencePreconditionStream struct {
	base       io.Reader
	blobDigest digest.Digest
}

func (r *existencePreconditionStream) Read(p []byte) (int, error) {
	n, err := r.base.Read(p)
	if err != nil && err != io.EOF {
		err = FailedPreconditionOnMissingBlob(r.blobDigest, err)
	}
	return n, err
}
