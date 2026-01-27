package pool

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// HoleSource is used by implementations of FilePool to provide the
// contents of parts of a file to which no data has been written
// explicitly (i.e., the holes of a file). This can be used to create
// files that provide copy-on-write behaviour.
//
// Implementations of FileReader are supposed to behave as ordinary
// FileReaders, with two differences:
//
//   - ReadAt() never returns io.EOF. Instead, if an attempt is made to
//     read past the end of the file, an infinite stream of null bytes
//     is returned.
//
//   - Truncate() can be used to remove data at the end of the HoleSource.
//     Subsequent attempts to read data must return null bytes.
type HoleSource interface {
	io.Closer
	io.ReaderAt

	GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error)
	Truncate(int64) error
}

// ZeroHoleSource is a primitive implementation of HoleSource that
// returns null bytes at any given offset. This is sufficient for cases
// where FilePool is only used to create files that are initially empty.
var ZeroHoleSource HoleSource = zeroHoleSource{}

type zeroHoleSource struct{}

func (zeroHoleSource) Close() error {
	return nil
}

func (zeroHoleSource) Truncate(size int64) error {
	return nil
}

func (zeroHoleSource) ReadAt(p []byte, off int64) (int, error) {
	clear(p)
	return len(p), nil
}

func (zeroHoleSource) GetNextRegionOffset(off int64, regionType filesystem.RegionType) (int64, error) {
	switch regionType {
	case filesystem.Data:
		return 0, io.EOF
	case filesystem.Hole:
		return off, nil
	default:
		panic("Unknown region type")
	}
}
