package cleaner

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// NewDirectoryCleaner creates a Cleaner that can remove all files
// within a given directory. It can, for example, be used to remove
// files from build directories and system temporary directories.
func NewDirectoryCleaner(directory filesystem.Directory, path string) Cleaner {
	return func(ctx context.Context) error {
		if err := directory.RemoveAllChildren(); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to clean directory %#v", path)
		}
		return nil
	}
}
