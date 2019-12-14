package builder

import (
	"context"
	"path"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type naiveInputRootPopulator struct {
	contentAddressableStorage cas.ContentAddressableStorage
}

// NewNaiveInputRootPopulator creates an input root populator that
// initializes the build directory of an action as simple as possible.
// Namely, it recursively loads all directories from the Content
// Addressable Storage (CAS) and requests that all of their files are
// copied into the build directory.
//
// This implementation is intended to be used in combination with
// regular local file systems. The downside of such file systems is that
// we cannot populate them on demand. All of the input files must be
// present before invoking the build action.
func NewNaiveInputRootPopulator(contentAddressableStorage cas.ContentAddressableStorage) InputRootPopulator {
	return &naiveInputRootPopulator{
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (ex *naiveInputRootPopulator) populateInputDirectory(ctx context.Context, digest *util.Digest, inputDirectory filesystem.Directory, components []string) error {
	// Obtain directory.
	directory, err := ex.contentAddressableStorage.GetDirectory(ctx, digest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain input directory %#v", path.Join(components...))
	}

	// Create children.
	for _, file := range directory.Files {
		childComponents := append(components, file.Name)
		childDigest, err := digest.NewDerivedDigest(file.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input file %#v", path.Join(childComponents...))
		}
		if err := ex.contentAddressableStorage.GetFile(ctx, childDigest, inputDirectory, file.Name, file.IsExecutable); err != nil {
			return util.StatusWrapf(err, "Failed to obtain input file %#v", path.Join(childComponents...))
		}
	}
	for _, directory := range directory.Directories {
		childComponents := append(components, directory.Name)
		childDigest, err := digest.NewDerivedDigest(directory.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input directory %#v", path.Join(childComponents...))
		}
		if err := inputDirectory.Mkdir(directory.Name, 0777); err != nil {
			return util.StatusWrapf(err, "Failed to create input directory %#v", path.Join(childComponents...))
		}
		childDirectory, err := inputDirectory.Enter(directory.Name)
		if err != nil {
			return util.StatusWrapf(err, "Failed to enter input directory %#v", path.Join(childComponents...))
		}
		err = ex.populateInputDirectory(ctx, childDigest, childDirectory, childComponents)
		childDirectory.Close()
		if err != nil {
			return err
		}
	}
	for _, symlink := range directory.Symlinks {
		childComponents := append(components, symlink.Name)
		if err := inputDirectory.Symlink(symlink.Target, symlink.Name); err != nil {
			return util.StatusWrapf(err, "Failed to create input symlink %#v", path.Join(childComponents...))
		}
	}
	return nil
}

func (ex *naiveInputRootPopulator) PopulateInputRoot(ctx context.Context, filePool re_filesystem.FilePool, digest *util.Digest, inputDirectory filesystem.Directory) error {
	return ex.populateInputDirectory(ctx, digest, inputDirectory, []string{"."})
}
