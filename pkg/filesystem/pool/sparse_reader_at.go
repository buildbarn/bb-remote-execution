package pool

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// SparseReaderAt is an interface that describes a ReaderAt with
// sparseness support. A typical implementation of this interface is a
// file. Reading from any hole in the file must return zeroes.
type SparseReaderAt interface {
	io.ReaderAt
	GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error)
}
