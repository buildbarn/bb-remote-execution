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
	GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error)
}
