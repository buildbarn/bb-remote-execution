package builder

import (
	"context"
	"path"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type naiveBuildDirectory struct {
	filesystem.DirectoryCloser
	contentAddressableStorage cas.ContentAddressableStorage
}

// NewNaiveBuildDirectory creates a BuildDirectory that is backed by a
// simple filesystem.Directory with all of the operations implemented in
// a naive way. Namely, MergeDirectoryContents() recursively loads all
// directories from the Content Addressable Storage (CAS) and requests
// that all of their files are copied into the build directory.
//
// This implementation is intended to be used in combination with
// regular local file systems. The downside of such file systems is that
// we cannot populate them on demand. All of the input files must be
// present before invoking the build action.
func NewNaiveBuildDirectory(directory filesystem.DirectoryCloser, contentAddressableStorage cas.ContentAddressableStorage) BuildDirectory {
	return &naiveBuildDirectory{
		DirectoryCloser:           directory,
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (d *naiveBuildDirectory) EnterBuildDirectory(name string) (BuildDirectory, error) {
	child, err := d.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	return &naiveBuildDirectory{
		DirectoryCloser:           child,
		contentAddressableStorage: d.contentAddressableStorage,
	}, nil
}

func (d *naiveBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	// Simply ignore the provided hooks, as POSIX offers no way to
	// install them. This means no quota enforcement and detection
	// of I/O errors is performed.
}

func (d *naiveBuildDirectory) mergeDirectoryContents(ctx context.Context, digest digest.Digest, inputDirectory filesystem.Directory, components []string) error {
	// Obtain directory.
	directory, err := d.contentAddressableStorage.GetDirectory(ctx, digest)
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
		if err := d.contentAddressableStorage.GetFile(ctx, childDigest, inputDirectory, file.Name, file.IsExecutable); err != nil {
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
		childDirectory, err := inputDirectory.EnterDirectory(directory.Name)
		if err != nil {
			return util.StatusWrapf(err, "Failed to enter input directory %#v", path.Join(childComponents...))
		}
		err = d.mergeDirectoryContents(ctx, childDigest, childDirectory, childComponents)
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

func (d *naiveBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error {
	return d.mergeDirectoryContents(ctx, digest, d.DirectoryCloser, []string{"."})
}
