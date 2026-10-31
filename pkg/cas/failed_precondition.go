package cas

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FailedPreconditionOnMissingBlob converts a NotFound error that was
// observed while reading a blob into a FAILED_PRECONDITION error,
// annotating it with a PreconditionFailure that identifies the blob as
// being missing. This is used by workers to make Execution::Execute()
// comply to the protocol: any blob needed by a build action that is not
// present in the CAS must be reported as such, so that the client can
// upload it again.
//
// Because only the caller knows which blob was being read (a chunk read
// failing underneath a blob read should refer to the blob), this
// function is to be called at blob-level read sites, not wrapped
// around storage backends.
func FailedPreconditionOnMissingBlob(d digest.Digest, observedErr error) error {
	s := status.Convert(observedErr)
	if s.Code() != codes.NotFound {
		return observedErr
	}
	s, err := status.New(codes.FailedPrecondition, s.Message()).WithDetails(
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{
					Type: "MISSING",
					Subject: digest.NewInstanceNamePatcher(d.GetInstanceName(), digest.EmptyInstanceName).
						PatchDigest(d).
						GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
				},
			},
		},
	)
	if err != nil {
		return err
	}
	return s.Err()
}
