package builder

import (
	"context"
	"path"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type naiveBuildDirectory struct {
	filesystem.DirectoryCloser
	contentAddressableStorage cas.ContentAddressableStorage
	sharedActionQueue         *re_util.SharedActionQueue
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
func NewNaiveBuildDirectory(directory filesystem.DirectoryCloser, contentAddressableStorage cas.ContentAddressableStorage, sharedActionQueue *re_util.SharedActionQueue) BuildDirectory {
	return &naiveBuildDirectory{
		DirectoryCloser:           directory,
		contentAddressableStorage: contentAddressableStorage,
		sharedActionQueue:         sharedActionQueue,
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
		sharedActionQueue:         d.sharedActionQueue,
	}, nil
}

func (d *naiveBuildDirectory) InstallHooks(filePool re_filesystem.FilePool) {
	// Simply ignore the provided hooks, as POSIX offers no way to
	// install them. This means no quota enforcement is performed.
}

func (d *naiveBuildDirectory) mergeDirectoryContents(ctx context.Context, digest digest.Digest, inputDirectory filesystem.Directory, components []string) error {
	// Obtain directory.
	directory, err := d.contentAddressableStorage.GetDirectory(ctx, digest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain input directory %#v", path.Join(components...))
	}

	actionGroup := re_util.ActionGroup{}

	for _, file := range directory.Files {
		childComponents := append([]string(nil), components...) // We need a copy of our slice to pass to go routine.
		childComponents = append(childComponents, file.Name)
		childDigest, err := digest.NewDerivedDigest(file.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input file %#v", path.Join(childComponents...))
		}
		// Extract values out of object so we don't end up with stale objects when we pass into go routine.
		fileName := file.Name
		fileIsExecutable := file.IsExecutable

		actionGroup = append(actionGroup, func(actionContext *re_util.ActionContext) {
			if err := d.contentAddressableStorage.GetFile(actionContext.Ctx(), childDigest, inputDirectory, fileName, fileIsExecutable); err != nil {
				actionContext.Done(util.StatusWrapf(err, "Failed to obtain input file %#v", path.Join(childComponents...)))
				return
			}
			actionContext.Done(nil)
		})
	}

	for _, directory := range directory.Directories {
		childComponents := append([]string(nil), components...) // We need a copy of our slice to pass to go routine.
		childComponents = append(childComponents, directory.Name)
		childDigest, err := digest.NewDerivedDigest(directory.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input directory %#v", path.Join(childComponents...))
		}
		if err := inputDirectory.Mkdir(directory.Name, 0777); err != nil {
			return util.StatusWrapf(err, "Failed to create input directory %#v", path.Join(childComponents...))
		}
		directoryName := directory.Name // Ensure we don't grab the reference to an object.
		actionGroup = append(actionGroup, func(actionContext *re_util.ActionContext) {
			childDirectory, err := inputDirectory.EnterDirectory(directoryName)
			if err != nil {
				actionContext.Done(util.StatusWrapf(err, "Failed to enter input directory %#v", path.Join(childComponents...)))
				return
			}
			// We don't want to close the folder's handle until our children are done, however we also don't
			// want to hold a slot of the SharedActionQueue (or we might deadlock). Since 'mergeDirectoryContents'
			// is a blocking function, we can notify the ActionContext that we want to release our hold on a slot
			// and let a new goroutine block on 'mergeDirectoryContents' for us then call the result handler
			// when we have finished.
			doneHandler := actionContext.Defer()
			go func() {
				defer childDirectory.Close()
				err := d.mergeDirectoryContents(actionContext.Ctx(), childDigest, childDirectory, childComponents)
				doneHandler(err)
			}()
		})
	}

	// Now wait for all of our queued actions. If any of them
	// error the first error is returned.
	err = d.sharedActionQueue.ProcessActionQueue(ctx, actionGroup).WaitForResult()
	if err != nil {
		return err
	}

	// We don't need to fetch symlinks, so link them now.
	for _, symlink := range directory.Symlinks {
		childComponents := append(components, symlink.Name)
		if err := inputDirectory.Symlink(symlink.Target, symlink.Name); err != nil {
			return util.StatusWrapf(err, "Failed to create input symlink %#v", path.Join(childComponents...))
		}
	}
	return nil
}

func (d *naiveBuildDirectory) MergeDirectoryContents(ctx context.Context, digest digest.Digest) error {
	return d.mergeDirectoryContents(ctx, digest, d.DirectoryCloser, []string{"."})
}
