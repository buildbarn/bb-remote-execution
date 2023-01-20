package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type existencePreconditionBlobAccess struct {
	blobstore.BlobAccess
}

// NewExistencePreconditionBlobAccess wraps a BlobAccess into a version
// that returns GRPC status code "FAILED_PRECONDITION" instead of
// "NOT_FOUND" for Get() operations. This is used by worker processes to
// make Execution::Execute() comply to the protocol.
func NewExistencePreconditionBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return &existencePreconditionBlobAccess{
		BlobAccess: blobAccess,
	}
}

func (ba *existencePreconditionBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.BlobAccess.Get(ctx, digest),
		existencePreconditionErrorHandler{digest: digest})
}

func (ba *existencePreconditionBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.BlobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		existencePreconditionErrorHandler{digest: parentDigest})
}

type existencePreconditionErrorHandler struct {
	digest digest.Digest
}

func (eh existencePreconditionErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	if s := status.Convert(observedErr); s.Code() == codes.NotFound {
		s, err := status.New(codes.FailedPrecondition, s.Message()).WithDetails(
			&errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{
						Type: "MISSING",
						Subject: digest.NewInstanceNamePatcher(eh.digest.GetInstanceName(), digest.EmptyInstanceName).
							PatchDigest(eh.digest).
							GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
					},
				},
			})
		if err != nil {
			return nil, err
		}
		return nil, s.Err()
	}
	return nil, observedErr
}

func (eh existencePreconditionErrorHandler) Done() {}
