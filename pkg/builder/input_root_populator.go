package builder

import (
	"context"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// InputRootPopulator implements the strategy for populating the build
// directory with the desired contents, so that it may be built. The
// simplest way of populating the input root is to recursively create
// all files and directories. A more complex strategy may use
// parallelism.
type InputRootPopulator interface {
	PopulateInputRoot(ctx context.Context, filePool re_filesystem.FilePool, digest digest.Digest, inputRoot filesystem.Directory) error
}
