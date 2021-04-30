package filesystem

import (
	"io"
	"os"
	"strconv"

	"github.com/buildbarn/bb-storage/pkg/atomic"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type directoryBackedFilePool struct {
	directory filesystem.Directory

	nextID atomic.Uint64
}

// NewDirectoryBackedFilePool creates a FilePool that stores all data
// written to files into a single directory on disk. Files stored in the
// underlying directory are simply identified by an incrementing number.
//
// As many files may exist at a given point in time, this implementation
// does not keep any backing files open. This would exhaust the worker's
// file descriptor table. Files are opened on demand.
//
// TODO: Maybe use an eviction.Set to keep a small number of files open?
func NewDirectoryBackedFilePool(directory filesystem.Directory) FilePool {
	return &directoryBackedFilePool{
		directory: directory,
	}
}

func (fp *directoryBackedFilePool) NewFile() (filesystem.FileReadWriter, error) {
	return &lazyOpeningSelfDeletingFile{
		directory: fp.directory,
		name:      path.MustNewComponent(strconv.FormatUint(fp.nextID.Add(1), 10)),
	}, nil
}

// lazyOpeningSelfDeletingFile is a file descriptor that forwards
// operations to a file that is opened on demand. Upon closure, the
// underlying file is unlinked.
type lazyOpeningSelfDeletingFile struct {
	directory filesystem.Directory
	name      path.Component
}

func (f *lazyOpeningSelfDeletingFile) Close() error {
	if err := f.directory.Remove(f.name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (f *lazyOpeningSelfDeletingFile) ReadAt(p []byte, off int64) (int, error) {
	fh, err := f.directory.OpenRead(f.name)
	if os.IsNotExist(err) {
		// Empty file that doesn't explicitly exist in the
		// backing store yet. Treat it as if it's a zero-length
		// file.
		return 0, io.EOF
	} else if err != nil {
		return 0, err
	}
	defer fh.Close()
	return fh.ReadAt(p, off)
}

func (f *lazyOpeningSelfDeletingFile) Sync() error {
	// Because FilePool does not provide any persistency, there is
	// no need to synchronize any data.
	return nil
}

func (f *lazyOpeningSelfDeletingFile) Truncate(size int64) error {
	fh, err := f.directory.OpenWrite(f.name, filesystem.CreateReuse(0o600))
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Truncate(size)
}

func (f *lazyOpeningSelfDeletingFile) WriteAt(p []byte, off int64) (int, error) {
	fh, err := f.directory.OpenWrite(f.name, filesystem.CreateReuse(0o600))
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	return fh.WriteAt(p, off)
}
