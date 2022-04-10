package builder

import (
	"context"
	"os"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type virtualBuildDirectoryOptions struct {
	directoryFetcher          cas.DirectoryFetcher
	contentAddressableStorage blobstore.BlobAccess
	symlinkFactory            virtual.SymlinkFactory
	characterDeviceFactory    virtual.CharacterDeviceFactory
	handleAllocator           virtual.StatefulHandleAllocator
}

type virtualBuildDirectory struct {
	virtual.PrepopulatedDirectory
	options *virtualBuildDirectoryOptions
}

// NewVirtualBuildDirectory creates a BuildDirectory that is backed by a
// virtual.PrepopulatedDirectory. Instead of creating all files in the
// input root explicitly, it calls PrepopulatedDirectory.CreateChildren
// to add special file and directory nodes whose contents are read on
// demand.
func NewVirtualBuildDirectory(directory virtual.PrepopulatedDirectory, directoryFetcher cas.DirectoryFetcher, contentAddressableStorage blobstore.BlobAccess, symlinkFactory virtual.SymlinkFactory, characterDeviceFactory virtual.CharacterDeviceFactory, handleAllocator virtual.StatefulHandleAllocator) BuildDirectory {
	return &virtualBuildDirectory{
		PrepopulatedDirectory: directory,
		options: &virtualBuildDirectoryOptions{
			directoryFetcher:          directoryFetcher,
			contentAddressableStorage: contentAddressableStorage,
			symlinkFactory:            symlinkFactory,
			characterDeviceFactory:    characterDeviceFactory,
			handleAllocator:           handleAllocator,
		},
	}
}

func (d *virtualBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	directory, _, err := d.LookupChild(name)
	if err != nil {
		return nil, err
	}
	if directory == nil {
		return nil, syscall.ENOTDIR
	}
	return &virtualBuildDirectory{
		PrepopulatedDirectory: directory,
		options:               d.options,
	}, nil
}

func (d *virtualBuildDirectory) Close() error {
	// Virtual directories do not need to be released explicitly.
	return nil
}

func (d *virtualBuildDirectory) EnterParentPopulatableDirectory(name path.Component) (ParentPopulatableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *virtualBuildDirectory) EnterUploadableDirectory(name path.Component) (UploadableDirectory, error) {
	return d.EnterBuildDirectory(name)
}

func (d *virtualBuildDirectory) InstallHooks(filePool re_filesystem.FilePool, errorLogger util.ErrorLogger) {
	d.PrepopulatedDirectory.InstallHooks(
		virtual.NewHandleAllocatingFileAllocator(
			virtual.NewPoolBackedFileAllocator(filePool, errorLogger),
			d.options.handleAllocator),
		errorLogger)
}

func (d *virtualBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, digest digest.Digest) error {
	initialContentsFetcher := virtual.NewCASInitialContentsFetcher(
		ctx,
		cas.NewDecomposedDirectoryWalker(d.options.directoryFetcher, digest),
		virtual.NewStatelessHandleAllocatingCASFileFactory(
			virtual.NewBlobAccessCASFileFactory(
				ctx,
				d.options.contentAddressableStorage,
				errorLogger),
			d.options.handleAllocator.New()),
		d.options.symlinkFactory,
		digest.GetInstanceName())
	children, err := initialContentsFetcher.FetchContents()
	if err != nil {
		return err
	}
	return d.CreateChildren(children, false)
}

func (d *virtualBuildDirectory) UploadFile(ctx context.Context, name path.Component, digestFunction digest.Function) (digest.Digest, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return digest.BadDigest, err
	}
	if leaf == nil {
		return digest.BadDigest, syscall.EISDIR
	}
	return leaf.UploadFile(ctx, d.options.contentAddressableStorage, digestFunction)
}

func (d *virtualBuildDirectory) Lstat(name path.Component) (filesystem.FileInfo, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return filesystem.FileInfo{}, err
	}
	if leaf == nil {
		return filesystem.NewFileInfo(name, filesystem.FileTypeDirectory, false), nil
	}
	return virtual.GetFileInfo(name, leaf), nil
}

func (d *virtualBuildDirectory) Mkdir(name path.Component, mode os.FileMode) error {
	return d.CreateChildren(map[path.Component]virtual.InitialNode{
		name: {
			Directory: virtual.EmptyInitialContentsFetcher,
		},
	}, false)
}

func (d *virtualBuildDirectory) Mknod(name path.Component, perm os.FileMode, deviceNumber filesystem.DeviceNumber) error {
	if perm&os.ModeType != os.ModeDevice|os.ModeCharDevice {
		return status.Error(codes.InvalidArgument, "The provided file mode is not for a character device")
	}
	characterDevice := d.options.characterDeviceFactory.LookupCharacterDevice(deviceNumber)
	if err := d.CreateChildren(map[path.Component]virtual.InitialNode{
		name: {Leaf: characterDevice},
	}, false); err != nil {
		characterDevice.Unlink()
		return err
	}
	return nil
}

func (d *virtualBuildDirectory) Readlink(name path.Component) (string, error) {
	_, leaf, err := d.LookupChild(name)
	if err != nil {
		return "", err
	}
	if leaf == nil {
		return "", syscall.EISDIR
	}
	return leaf.Readlink()
}
