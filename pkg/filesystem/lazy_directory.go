package filesystem

import (
	"os"
	"time"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// DirectoryOpener is a callback that is used by LazyDirectory to open
// the underlying directory on demand.
type DirectoryOpener func() (filesystem.DirectoryCloser, error)

type lazyDirectory struct {
	directoryOpener DirectoryOpener
}

// NewLazyDirectory creates a directory handle that forwards all calls
// to a directory that is created on demand. The primary use case for
// this adapter is for the FUSE-based runner.
//
// A runner process may get started before the worker is able to create
// its FUSE mount point. This would cause the runner to obtain a handle
// to the build directory underneath the FUSE mount, causing builds to
// fail due to missing input files.
//
// Relatedly, if the worker would start before the runner, but end up
// crashing/restarting, the runner would still have a directory handle
// pointing to a stale FUSE mount.
//
// This wrapper prevents these problems by ensuring that we never hold
// on to a file descriptor to the build directory.
func NewLazyDirectory(directoryOpener DirectoryOpener) filesystem.Directory {
	return &lazyDirectory{
		directoryOpener: directoryOpener,
	}
}

func (d *lazyDirectory) openUnderlying() (filesystem.DirectoryCloser, error) {
	underlying, err := d.directoryOpener()
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to open underlying directory")
	}
	return underlying, nil
}

func (d *lazyDirectory) EnterDirectory(name string) (filesystem.DirectoryCloser, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.EnterDirectory(name)
}

func (d *lazyDirectory) OpenAppend(name string, creationMode filesystem.CreationMode) (filesystem.FileAppender, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.OpenAppend(name, creationMode)
}

func (d *lazyDirectory) OpenRead(name string) (filesystem.FileReader, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.OpenRead(name)
}

func (d *lazyDirectory) OpenReadWrite(name string, creationMode filesystem.CreationMode) (filesystem.FileReadWriter, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.OpenReadWrite(name, creationMode)
}

func (d *lazyDirectory) OpenWrite(name string, creationMode filesystem.CreationMode) (filesystem.FileWriter, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.OpenWrite(name, creationMode)
}

func (d *lazyDirectory) Link(oldName string, newDirectory filesystem.Directory, newName string) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Link(oldName, newDirectory, newName)
}

func (d *lazyDirectory) Lstat(name string) (filesystem.FileInfo, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return filesystem.FileInfo{}, err
	}
	defer underlying.Close()
	return underlying.Lstat(name)
}

func (d *lazyDirectory) Mkdir(name string, perm os.FileMode) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Mkdir(name, perm)
}

func (d *lazyDirectory) Mknod(name string, perm os.FileMode, dev int) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Mknod(name, perm, dev)
}

func (d *lazyDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return nil, err
	}
	defer underlying.Close()
	return underlying.ReadDir()
}

func (d *lazyDirectory) Readlink(name string) (string, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return "", err
	}
	defer underlying.Close()
	return underlying.Readlink(name)
}

func (d *lazyDirectory) Remove(name string) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Remove(name)
}

func (d *lazyDirectory) RemoveAll(name string) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.RemoveAll(name)
}

func (d *lazyDirectory) RemoveAllChildren() error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.RemoveAllChildren()
}

func (d *lazyDirectory) Rename(oldName string, newDirectory filesystem.Directory, newName string) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Rename(oldName, newDirectory, newName)
}

func (d *lazyDirectory) Symlink(oldName, newName string) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Symlink(oldName, newName)
}

func (d *lazyDirectory) Sync() error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Sync()
}

func (d *lazyDirectory) Chtimes(name string, atime, mtime time.Time) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Chtimes(name, atime, mtime)
}

func (d *lazyDirectory) IsWritable() (bool, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return false, err
	}
	defer underlying.Close()
	return underlying.IsWritable()
}

func (d *lazyDirectory) IsWritableChild(name string) (bool, error) {
	underlying, err := d.openUnderlying()
	if err != nil {
		return false, err
	}
	defer underlying.Close()
	return underlying.IsWritableChild(name)
}

func (d *lazyDirectory) Apply(arg interface{}) error {
	underlying, err := d.openUnderlying()
	if err != nil {
		return err
	}
	defer underlying.Close()
	return underlying.Apply(arg)
}
