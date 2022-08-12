package cas

import (
	"context"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type decomposedDirectoryWalker struct {
	fetcher DirectoryFetcher
	digest  digest.Digest
}

// NewDecomposedDirectoryWalker creates a DirectoryWalker that assumes
// that all Directory messages are stored as separate objects in the
// Content Addressable Storage (CAS). This is the case for input roots
// of build actions.
func NewDecomposedDirectoryWalker(fetcher DirectoryFetcher, digest digest.Digest) DirectoryWalker {
	return &decomposedDirectoryWalker{
		fetcher: fetcher,
		digest:  digest,
	}
}

func (dw *decomposedDirectoryWalker) GetDirectory(ctx context.Context) (*remoteexecution.Directory, error) {
	return dw.fetcher.Get(ctx, dw.digest)
}

func (dw *decomposedDirectoryWalker) GetChild(digest digest.Digest) DirectoryWalker {
	return &decomposedDirectoryWalker{
		fetcher: dw.fetcher,
		digest:  digest,
	}
}

func (dw *decomposedDirectoryWalker) GetDescription() string {
	return fmt.Sprintf("Directory %#v", dw.digest.String())
}

func (dw *decomposedDirectoryWalker) GetContainingDigest() digest.Digest {
	return dw.digest
}
