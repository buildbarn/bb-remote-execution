package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// ContentAddressableStorage provides typed access to a Bazel Content
// Addressable Storage (CAS).
//
// TODO: Now that we have Buffer.ToProto(), this interface has become a
// lot less useful. Should we remove this, just like how we removed
// ActionCache when we added Buffer.ToActionResult()?
type ContentAddressableStorage interface {
	GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error)
	GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error
	PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error)
}
