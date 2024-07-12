package builder

import (
	"context"
	"io"
	"math"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type naiveBuildDirectoryOptions struct {
	directoryFetcher          cas.DirectoryFetcher
	fileFetcher               cas.FileFetcher
	fileFetcherSemaphore      *semaphore.Weighted
	contentAddressableStorage blobstore.BlobAccess
}

type naiveBuildDirectory struct {
	filesystem.DirectoryCloser
	options *naiveBuildDirectoryOptions
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
func NewNaiveBuildDirectory(directory filesystem.DirectoryCloser, directoryFetcher cas.DirectoryFetcher, fileFetcher cas.FileFetcher, fileFetcherSemaphore *semaphore.Weighted, contentAddressableStorage blobstore.BlobAccess) BuildDirectory {
	return &naiveBuildDirectory{
		DirectoryCloser: directory,
		options: &naiveBuildDirectoryOptions{
			directoryFetcher:          directoryFetcher,
			fileFetcher:               fileFetcher,
			fileFetcherSemaphore:      fileFetcherSemaphore,
			contentAddressableStorage: contentAddressableStorage,
		},
	}
}

func (d *naiveBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	child, err := d.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	return &naiveBuildDirectory{
		DirectoryCloser: child,
		options:         d.options,
	}, nil
}

func (d *naiveBuildDirectory) EnterParentPopulatableDirectory(name path.Component) (ParentPopulatableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *naiveBuildDirectory) EnterUploadableDirectory(name path.Component) (UploadableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *naiveBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	// Simply ignore the provided hooks, as POSIX offers no way to
	// install them. This means no quota enforcement and detection
	// of I/O errors is performed.
}

func (d *naiveBuildDirectory) mergeDirectoryContents(ctx context.Context, group *errgroup.Group, digest digest.Digest, inputDirectory *filesystem.ReferenceCountedDirectoryCloser, pathTrace *path.Trace) error {
	// Obtain directory.
	options := d.options
	directory, err := options.directoryFetcher.GetDirectory(ctx, digest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain input directory %#v", pathTrace.GetUNIXString())
	}

	// Create children.
	digestFunction := digest.GetDigestFunction()
	for _, file := range directory.Files {
		component, ok := path.NewComponent(file.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", file.Name)
		}
		childPathTrace := pathTrace.Append(component)
		childDigest, err := digestFunction.NewDigestFromProto(file.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input file %#v", childPathTrace.GetUNIXString())
		}

		// Download individual input files in parallel.
		if err := util.AcquireSemaphore(ctx, options.fileFetcherSemaphore, 1); err != nil {
			return err
		}
		downloadDirectory := inputDirectory.Duplicate()
		group.Go(func() error {
			errGetFile := options.fileFetcher.GetFile(ctx, childDigest, downloadDirectory, component, file.IsExecutable)
			errClose := downloadDirectory.Close()
			options.fileFetcherSemaphore.Release(1)
			if errGetFile != nil {
				return util.StatusWrapf(errGetFile, "Failed to obtain input file %#v", childPathTrace.GetUNIXString())
			}
			if errClose != nil {
				return util.StatusWrapf(err, "Failed to close input directory %#v", pathTrace.GetUNIXString())
			}
			return nil
		})
	}
	for _, directory := range directory.Directories {
		component, ok := path.NewComponent(directory.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v has an invalid name", directory.Name)
		}
		childPathTrace := pathTrace.Append(component)
		childDigest, err := digestFunction.NewDigestFromProto(directory.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input directory %#v", childPathTrace.GetUNIXString())
		}
		if err := inputDirectory.Mkdir(component, 0o777); err != nil {
			return util.StatusWrapf(err, "Failed to create input directory %#v", childPathTrace.GetUNIXString())
		}
		childDirectory, err := inputDirectory.EnterDirectory(component)
		if err != nil {
			return util.StatusWrapf(err, "Failed to enter input directory %#v", childPathTrace.GetUNIXString())
		}
		refcountedDirectory := filesystem.NewReferenceCountedDirectoryCloser(childDirectory)
		errMerge := d.mergeDirectoryContents(ctx, group, childDigest, refcountedDirectory, childPathTrace)
		errClose := refcountedDirectory.Close()
		if errMerge != nil {
			return errMerge
		}
		if errClose != nil {
			return util.StatusWrapf(err, "Failed to close input directory %#v", childPathTrace.GetUNIXString())
		}
	}
	for _, symlink := range directory.Symlinks {
		component, ok := path.NewComponent(symlink.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Symlink %#v has an invalid name", symlink.Name)
		}
		childPathTrace := pathTrace.Append(component)
		if err := inputDirectory.Symlink(path.NewUNIXParser(symlink.Target), component); err != nil {
			return util.StatusWrapf(err, "Failed to create input symlink %#v", childPathTrace.GetUNIXString())
		}
	}
	return nil
}

func (d *naiveBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest, monitor access.UnreadDirectoryMonitor) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return d.mergeDirectoryContents(groupCtx, group, digest, filesystem.NewReferenceCountedDirectoryCloser(d.DirectoryCloser), nil)
	})
	return group.Wait()
}

func (d *naiveBuildDirectory) UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
	file, err := d.OpenRead(name)
	if err != nil {
		return digest.BadDigest, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := digestFunction.NewGenerator(math.MaxInt64)
	sizeBytes, err := io.Copy(digestGenerator, io.NewSectionReader(file, 0, math.MaxInt64))
	if err != nil {
		file.Close()
		return digest.BadDigest, util.StatusWrap(err, "Failed to compute file digest")
	}
	blobDigest := digestGenerator.Sum()

	// Rewind and store it. Limit uploading to the size that was
	// used to compute the digest. This ensures uploads succeed,
	// even if more data gets appended in the meantime. This is not
	// uncommon, especially for stdout and stderr logs.
	if err := d.options.contentAddressableStorage.Put(
		ctx,
		blobDigest,
		buffer.NewCASBufferFromReader(
			blobDigest,
			newSectionReadCloser(file, 0, sizeBytes),
			buffer.UserProvided)); err != nil {
		return digest.BadDigest, util.StatusWrap(err, "Failed to upload file")
	}
	return blobDigest, nil
}

// newSectionReadCloser returns an io.ReadCloser that reads from r at a
// given offset, but stops with EOF after n bytes. This function is
// identical to io.NewSectionReader(), except that it provides an
// io.ReadCloser instead of an io.Reader.
func newSectionReadCloser(r filesystem.FileReader, off, n int64) io.ReadCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, off, n),
		Closer:        r,
	}
}
