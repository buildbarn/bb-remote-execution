package pool

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// FilePool is an allocator for temporary files. Files are created by
// calling NewFile(). They are automatically removed by calling Close().
//
// File handles returned by NewFile() are not thread-safe. Additional
// locking needs to be done at higher levels to permit safe concurrent
// access.
//
// If the sparseReaderAt parameter is not nil the file is created as if
// it had the initial content of the supplied SparseReaderAt. The size
// parameter must be less than or equal to the size of the underlying
// data. If the sparseReaderAt parameter is nil the file is created
// empty.
type FilePool interface {
	NewFile(sparseReaderAt SparseReaderAt, size uint64) (filesystem.FileReadWriter, error)
}
