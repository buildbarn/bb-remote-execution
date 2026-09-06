package cas

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// ByteSliceFileReader implements the filesystem.FileReader interface
// for a byte slice, which is convenient to use for the BlobUploader
// when uploading small blobs to the cas.
type ByteSliceFileReader []byte

// Close is a noop for a ByteSliceFileReader
func (ByteSliceFileReader) Close() error { return nil }

// Len is the length of the original byte slice
func (b ByteSliceFileReader) Len() (int64, error) { return int64(len(b)), nil }

// ReadAt reads from the byte slice, taking care to return io.EOF for
// out of bounds reads.
func (b ByteSliceFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= int64(len(b)) {
		return 0, io.EOF
	}
	n = copy(p, b[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// GetNextRegionOffset treats the entire byte slice as dense.
func (b ByteSliceFileReader) GetNextRegionOffset(off int64, regionType filesystem.RegionType) (int64, error) {
	if off < 0 || off >= int64(len(b)) {
		return 0, io.EOF
	}
	if regionType == filesystem.Hole {
		return int64(len(b)), nil
	}
	return off, nil // Data
}

var _ filesystem.FileReader = ByteSliceFileReader([]byte(""))
