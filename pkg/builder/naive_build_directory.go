package builder

import (
	"context"
	"io"
	"math"
	"path"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
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

func (d *naiveBuildDirectory) EnterBuildDirectory(name string) (BuildDirectory, error) {
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

func (d *naiveBuildDirectory) EnterUploadableDirectory(name string) (UploadableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *naiveBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	// Simply ignore the provided hooks, as POSIX offers no way to
	// install them. This means no quota enforcement and detection
	// of I/O errors is performed.
}

func (d *naiveBuildDirectory) mergeDirectoryContents(ctx context.Context, digest digest.Digest, inputDirectory filesystem.Directory, components []string) error {
	// Obtain directory.
	directory, err := d.directoryFetcher.GetDirectory(ctx, digest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain input directory %#v", path.Join(components...))
	}

	// Create children.
	instanceName := digest.GetInstanceName()
	for _, file := range directory.Files {
		childComponents := append(components, file.Name)
		childDigest, err := instanceName.NewDigestFromProto(file.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to extract digest for input file %#v", path.Join(childComponents...))
		}
		if err := d.fileFetcher.GetFile(ctx, childDigest, inputDirectory, file.Name, file.IsExecutable); err != nil {
			return util.StatusWrapf(err, "Failed to obtain input file %#v", path.Join(childComponents...))
		}
	}
	for _, directory := range directory.Directories {
		childComponents := append(components, directory.Name)
		childDigest, err := instanceName.NewDigestFromProto(directory.Digest)
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

func (d *naiveBuildDirectory) UploadFile(ctx context.Context, name string, parentDigest digest.Digest) (digest.Digest, error) {
	return casPutFile(ctx, d.contentAddressableStorage, d, name, parentDigest)
}

func casPutFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error) {
	file, err := directory.OpenRead(name)
	if err != nil {
		return digest.BadDigest, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := parentDigest.NewGenerator()
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
	if err := contentAddressableStorage.Put(
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
func newSectionReadCloser(r filesystem.FileReader, off int64, n int64) io.ReadCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, off, n),
		Closer:        r,
	}
}
