//go:build darwin || linux
// +build darwin linux

package builder

import (
	"context"
	"os"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fuseBuildDirectoryOptions struct {
	directoryFetcher          cas.DirectoryFetcher
	contentAddressableStorage blobstore.BlobAccess
	inodeNumberTree           fuse.InodeNumberTree
}

type fuseBuildDirectory struct {
	fuse.PrepopulatedDirectory
	options *fuseBuildDirectoryOptions
}

// NewFUSEBuildDirectory creates a BuildDirectory that is backed by a
// fuse.PrepopulatedDirectory. Instead of creating all files in the
// input root explicitly, it calls PrepopulatedDirectory.CreateChildren
// to add special file and directory nodes whose contents are read on
// demand.
func NewFUSEBuildDirectory(directory fuse.PrepopulatedDirectory, directoryFetcher cas.DirectoryFetcher, contentAddressableStorage blobstore.BlobAccess, inodeNumberTree fuse.InodeNumberTree) BuildDirectory {
	return &fuseBuildDirectory{
		PrepopulatedDirectory: directory,
		options: &fuseBuildDirectoryOptions{
			directoryFetcher:          directoryFetcher,
			contentAddressableStorage: contentAddressableStorage,
			inodeNumberTree:           inodeNumberTree,
		},
	}
}

func (d *fuseBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	directory, _, err := d.LookupChild(name)
	if err != nil {
		return nil, err
	}
	if directory == nil {
		return nil, syscall.ENOTDIR
	}
	return &fuseBuildDirectory{
		PrepopulatedDirectory: directory,
		options:               d.options,
	}, nil
}

func (d *fuseBuildDirectory) Close() error {
	// FUSE directories do not need to be released explicitly.
	return nil
}

func (d *fuseBuildDirectory) EnterParentPopulatableDirectory(name path.Component) (ParentPopulatableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *fuseBuildDirectory) EnterUploadableDirectory(name path.Component) (UploadableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *fuseBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	d.PrepopulatedDirectory.InstallHooks(
		fuse.NewPoolBackedFileAllocator(filePool, errorLogger, random.FastThreadSafeGenerator),
		errorLogger)
}

func (d *fuseBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error {
	initialContentsFetcher := fuse.NewCASInitialContentsFetcher(
		ctx,
		cas.NewDecomposedDirectoryWalker(d.options.directoryFetcher, digest),
		fuse.NewBlobAccessCASFileFactory(
			ctx,
			d.options.contentAddressableStorage,
			errorLogger),
		digest.GetInstanceName())
	children, err := initialContentsFetcher.FetchContents()
	if err != nil {
		return err
	}
	return d.CreateChildren(children, false)
}

func (d *fuseBuildDirectory) UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function) (digest.Digest, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return digest.BadDigest, err
	}
	if leaf == nil {
		return digest.BadDigest, syscall.EISDIR
	}
	return leaf.UploadFile(ctx, d.options.contentAddressableStorage, digestFunction)
}

func (d *fuseBuildDirectory) Lstat(name path.Component) (filesystem.FileInfo, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return filesystem.FileInfo{}, err
	}
	if leaf == nil {
		return filesystem.NewFileInfo(name, filesystem.FileTypeDirectory), nil
	}
	return filesystem.NewFileInfo(name, leaf.GetFileType()), nil
}

func (d *fuseBuildDirectory) Mkdir(name path.Component, mode os.FileMode) error {
	return d.CreateChildren(map[path.Component]fuse.InitialNode{
		name: {
			Directory: fuse.EmptyInitialContentsFetcher,
		},
	}, false)
}

func (d *fuseBuildDirectory) Mknod(name path.Component, perm os.FileMode, dev int) error {
	if perm&os.ModeType != os.ModeDevice|os.ModeCharDevice {
		return status.Error(codes.InvalidArgument, "The provided file mode is not for a character device")
	}
	return d.CreateChildren(map[path.Component]fuse.InitialNode{
		name: {
			Leaf: fuse.NewDevice(d.options.inodeNumberTree.AddUint64(uint64(dev)).Get(), syscall.S_IFCHR, uint32(dev)),
		},
	}, false)
}

func (d *fuseBuildDirectory) Readlink(name path.Component) (string, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return "", err
	}
	if leaf == nil {
		return "", syscall.EISDIR
	}
	return leaf.Readlink()
}
