package filesystem

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type inMemoryFilePool struct{}

func (fp inMemoryFilePool) NewFile() (filesystem.FileReadWriter, error) {
	return &inMemoryFile{}, nil
}

type inMemoryFile struct {
	data []byte
}

func (f *inMemoryFile) Close() error {
	f.data = nil
	return nil
}

func (f *inMemoryFile) ReadAt(p []byte, off int64) (int, error) {
	if int(off) >= len(f.data) {
		return 0, io.EOF
	}
	if n := copy(p, f.data[off:]); n < len(p) {
		return n, io.EOF
	}
	return len(p), nil
}

func (f *inMemoryFile) Truncate(size int64) error {
	if len(f.data) >= int(size) {
		// Truncate the file.
		f.data = f.data[:size]
	} else {
		// Grow the file.
		f.data = append(f.data, make([]byte, int(size)-len(f.data))...)
	}
	return nil
}

func (f *inMemoryFile) WriteAt(p []byte, off int64) (int, error) {
	// Zero-sized writes should not cause the file to grow.
	if len(p) == 0 {
		return 0, nil
	}

	if size := int(off) + len(p); len(f.data) < size {
		// Grow the file.
		f.data = append(f.data, make([]byte, size-len(f.data))...)
	}
	return copy(f.data[off:], p), nil
}

// InMemoryFilePool is a FilePool that stores all data in memory.
var InMemoryFilePool FilePool = inMemoryFilePool{}
