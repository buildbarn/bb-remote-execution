package cas

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// ByteSliceFileReader implements the filesystem.FileReader interface
// for a byte slice, which is convenient to use for the BlobUploader
// when uploading small blobs to the cas.
type ByteSliceFileReader []byte

func (b ByteSliceFileReader) Close() error { return nil }

func (b ByteSliceFileReader) Len() (int64, error) { return int64(len(b)), nil }

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
