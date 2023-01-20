package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type suspendingDirectoryFetcher struct {
	base        DirectoryFetcher
	suspendable clock.Suspendable
}

// NewSuspendingDirectoryFetcher is a decorator for DirectoryFetcher
// that simply forwards all methods. Before and after each call, it
// suspends and resumes a clock.Suspendable object, respectively.
//
// This decorator is used in combination with SuspendableClock, allowing
// FUSE/NFSv4-based workers to compensate the execution timeout of build
// actions for any time spent loading directory contents of the input
// root.
func NewSuspendingDirectoryFetcher(base DirectoryFetcher, suspendable clock.Suspendable) DirectoryFetcher {
	return &suspendingDirectoryFetcher{
		base:        base,
		suspendable: suspendable,
	}
}

func (df *suspendingDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	df.suspendable.Suspend()
	defer df.suspendable.Resume()

	return df.base.GetDirectory(ctx, directoryDigest)
}

func (df *suspendingDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	df.suspendable.Suspend()
	defer df.suspendable.Resume()

	return df.base.GetTreeRootDirectory(ctx, treeDigest)
}

func (df *suspendingDirectoryFetcher) GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error) {
	df.suspendable.Suspend()
	defer df.suspendable.Resume()

	return df.base.GetTreeChildDirectory(ctx, treeDigest, childDigest)
}
