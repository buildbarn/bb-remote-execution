package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// DirectoryFetcher is responsible for fetching Directory messages from
// the Content Addressable Storage (CAS). These describe the layout of a
// single directory in a build action's input root.
type DirectoryFetcher interface {
	GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error)
	GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error)
	GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error)
}
