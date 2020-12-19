package filesystem

import (
	"os"
	"path/filepath"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckAllWritablePathsAreAllowed errors if any transitive child of
// currentPath (which dir is a handle to) is writable,
// other than those paths present in allowed. Both currentPath and all entries
// in allowed are expected to be absolute paths.
func CheckAllWritablePathsAreAllowed(dir filesystem.Directory, currentPath string, allowed map[string]struct{}) error {
	if !filepath.IsAbs(currentPath) {
		return status.Errorf(codes.InvalidArgument, "Want clean absolute path but was given %v", currentPath)
	}

	isWritable, err := dir.IsWritable()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "Failed to check writability of %v: %#v", currentPath, err)
	}
	if isWritable {
		return status.Errorf(codes.FailedPrecondition, "Directory %#v was writable", currentPath)
	}

	children, err := dir.ReadDir()
	if err != nil {
		return util.StatusWrapf(err, "Failed to read contents of directory %#v", err)
	}

	for _, child := range children {
		childPath := filepath.Join(currentPath, child.Name().String())
		if _, ok := allowed[childPath]; ok {
			continue
		}

		switch child.Type() {
		case filesystem.FileTypeSymlink:
			continue
		case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile, filesystem.FileTypeBlockDevice, filesystem.FileTypeCharacterDevice, filesystem.FileTypeFIFO, filesystem.FileTypeSocket:
			isWritable, err := dir.IsWritableChild(child.Name())
			if err != nil {
				return util.StatusWrapf(err, "Failed to check writability of path %#v", err)
			}
			if isWritable {
				return status.Errorf(codes.FailedPrecondition, "Path %s was writable", childPath)
			}
		case filesystem.FileTypeDirectory:
			// We do a writability check after entering the child dir.
			childDir, err := dir.EnterDirectory(child.Name())
			if os.IsPermission(err) {
				continue
			} else if err != nil {
				return status.Errorf(codes.Unknown, "Failed to enter directory %v: %#v", childPath, err)
			}
			err = CheckAllWritablePathsAreAllowed(childDir, childPath, allowed)
			childDir.Close()
			if err != nil {
				return err
			}
		default:
			return status.Errorf(codes.Unknown, "Unrecognized file type for file %v: %v", childPath, child.Type())
		}
	}
	return nil
}
