package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type existencePreconditionContentAddressableStorage struct {
	cas.ContentAddressableStorage
}

// NewExistencePreconditionContentAddressableStorage wraps a
// ContentAddressableStorage into a version that returns GRPC status
// code "FAILED_PRECONDITION" instead of "NOT_FOUND" for Get() style
// operations. This is used by worker processes to make
// Execution::Execute() comply to the protocol.
func NewExistencePreconditionContentAddressableStorage(contentAddressableStorage cas.ContentAddressableStorage) cas.ContentAddressableStorage {
	return &existencePreconditionContentAddressableStorage{
		ContentAddressableStorage: contentAddressableStorage,
	}
}

func (cas *existencePreconditionContentAddressableStorage) FetchChunk(ctx context.Context, d digest.Digest) ([]byte, error) {
	data, err := cas.ContentAddressableStorage.FetchChunk(ctx, d)
	if err != nil {
		return nil, toFailedPrecondition(d, err)
	}
	return data, nil
}

func (cas *existencePreconditionContentAddressableStorage) GetManifest(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	manifest, err := cas.ContentAddressableStorage.GetManifest(ctx, d)
	if err != nil {
		return nil, toFailedPrecondition(d, err)
	}
	return manifest, nil
}

func toFailedPrecondition(d digest.Digest, observedErr error) error {
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
