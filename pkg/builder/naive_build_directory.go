package builder

import (
	"context"
	"io"
	"math"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type naiveBuildDirectory struct {
	filesystem.DirectoryCloser
	directoryFetcher          cas.DirectoryFetcher
	fileFetcher               cas.FileFetcher
	contentAddressableStorage blobstore.BlobAccess
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
func NewNaiveBuildDirectory(directory filesystem.DirectoryCloser, directoryFetcher cas.DirectoryFetcher, fileFetcher cas.FileFetcher, contentAddressableStorage blobstore.BlobAccess) BuildDirectory {
	return &naiveBuildDirectory{
		DirectoryCloser:           directory,
		directoryFetcher:          directoryFetcher,
		fileFetcher:               fileFetcher,
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (d *naiveBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	child, err := d.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	return &naiveBuildDirectory{
		DirectoryCloser:           child,
		directoryFetcher:          d.directoryFetcher,
		fileFetcher:               d.fileFetcher,
		contentAddressableStorage: d.contentAddressableStorage,
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

func (d *naiveBuildDirectory) mergeDirectoryContents(ctx context.Context, digest digest.Digest, inputDirectory filesystem.Directory, pathTrace *path.Trace) error {
	// Obtain directory.
	directory, err := d.directoryFetcher.GetDirectory(ctx, digest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain input directory %#v", pathTrace.String())
	}

	// Create children.
	instanceName := digest.GetInstanceName()
	for _, file := range directory.Files {
		component, ok := path.NewComponent(file.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", file.Name)
		}
		childPathTrace := pathTrace.Append(component)
		childDigest, err := instanceName.NewDigestFromProto(file.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input file %#v", childPathTrace.String())
		}
		if err := d.fileFetcher.GetFile(ctx, childDigest, inputDirectory, component, file.IsExecutable); err != nil {
			return util.StatusWrapf(err, "Failed to obtain input file %#v", childPathTrace.String())
		}
	}
	for _, directory := range directory.Directories {
		component, ok := path.NewComponent(directory.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v has an invalid name", directory.Name)
		}
		childPathTrace := pathTrace.Append(component)
		childDigest, err := instanceName.NewDigestFromProto(directory.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input directory %#v", childPathTrace.String())
		}
		if err := inputDirectory.Mkdir(component, 0o777); err != nil {
			return util.StatusWrapf(err, "Failed to create input directory %#v", childPathTrace.String())
		}
		childDirectory, err := inputDirectory.EnterDirectory(component)
		if err != nil {
			return util.StatusWrapf(err, "Failed to enter input directory %#v", childPathTrace.String())
		}
		err = d.mergeDirectoryContents(ctx, childDigest, childDirectory, childPathTrace)
		childDirectory.Close()
		if err != nil {
			return err
		}
	}
	for _, symlink := range directory.Symlinks {
		component, ok := path.NewComponent(symlink.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Symlink %#v has an invalid name", symlink.Name)
		}
		childPathTrace := pathTrace.Append(component)
		if err := inputDirectory.Symlink(symlink.Target, component); err != nil {
			return util.StatusWrapf(err, "Failed to create input symlink %#v", childPathTrace.String())
		}
	}
	return nil
}

func (d *naiveBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error {
	return d.mergeDirectoryContents(ctx, digest, d.DirectoryCloser, nil)
}

func (d *naiveBuildDirectory) UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function) (digest.Digest, error) {
	file, err := d.OpenRead(name)
	if err != nil {
		return digest.BadDigest, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := digestFunction.NewGenerator()
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
	if err := d.contentAddressableStorage.Put(
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
