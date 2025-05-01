package pool

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TruncatableSparseReaderAt decorates a sparse readonly input source
// with a virtual truncate method. Its purpose is to be used as initial
// backing store for a file implementation that supports writing on top
// of a read only file.
//
// While the source is read-only the TruncatableSparseReaderAt can be
// truncated to a smaller size. When shrunk the truncated content will
// be supressed for future reads.
type TruncatableSparseReaderAt interface {
	SparseReaderAt
	Truncate(size int64) error
}

type truncatableSparseReaderAt struct {
	base        SparseReaderAt
	logicalSize int64
	sizeBytes   int64
}

// NewTruncatableSparseReaderAt creates a new truncatable sparse reader
// at. The supplied underlying sparse reader at must be at least
// sizeBytes long.
func NewTruncatableSparseReaderAt(underlying SparseReaderAt, sizeBytes int64) TruncatableSparseReaderAt {
	return &truncatableSparseReaderAt{
		base:        underlying,
		logicalSize: sizeBytes,
		sizeBytes:   sizeBytes,
	}
}

func (r *truncatableSparseReaderAt) Truncate(size int64) error {
	// Truncate the file to a given size, r.sizeBytes, which represents
	// the readable size of the underlying source can only be shrunk.
	// Once the file is shrunk future reads from the truncated region
	// will return zeroes.
	if size < 0 {
		return status.Errorf(codes.InvalidArgument, "cannot truncate to negative size %d", size)
	}
	if size < r.sizeBytes {
		r.sizeBytes = size
	}
	r.logicalSize = size
	return nil
}

// io.ReaderAt may or may not return EOF if the read is full but at the
// end of the of the input source. This wrapper removes that specific
// behavior since we do not want to propagate EOF from underlying.
func (r *truncatableSparseReaderAt) readUnderlyingSuppressEOF(p []byte, offset int64) (int, error) {
	n, err := r.base.ReadAt(p, offset)
	if err == io.EOF && n == len(p) {
		return n, nil
	}
	return n, err
}

// Read from the underlying source for the parts which are within the
// sizeBytes. Pad the rest with zeroes up to logicalSize.
func (r *truncatableSparseReaderAt) ReadAt(p []byte, off int64) (int, error) {
	// Short circuit calls that are out of bounds.
	if off < 0 {
		return 0, status.Errorf(codes.InvalidArgument, "Negative read offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Limit the read operation to the size of the file. Already
	// determine whether this operation will return nil or io.EOF.
	if uint64(off) >= uint64(r.logicalSize) {
		return 0, io.EOF
	}
	var success error
	if end := off + int64(len(p)); end >= r.logicalSize {
		success = io.EOF
		p = p[:r.logicalSize-off]
	}

	// Bytes to read from the underlying stream.
	n1 := min(int64(len(p)), max(r.sizeBytes-off, 0))
	n2, err := r.readUnderlyingSuppressEOF(p[:n1], off)
	if int64(n2) != n1 || err != nil {
		return n2, err
	}
	for i := range len(p[n1:]) {
		p[n1+int64(i)] = 0
	}
	return len(p), success
}

func (r *truncatableSparseReaderAt) GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error) {
	// For indexes within r.sizeBytes this interface has holes where the
	// underlying interface has holes and data where the underlying
	// interface has data. For indexes outside of r.sizeBytes this
	// interface is all holes.
	if offset < 0 {
		return 0, status.Errorf(codes.InvalidArgument, "negative offset %d", offset)
	}
	if offset >= r.logicalSize {
		return 0, io.EOF
	}
	switch regionType {
	case filesystem.Data:
		if offset < r.sizeBytes {
			innerOffset, err := r.base.GetNextRegionOffset(offset, regionType)
			if innerOffset < r.sizeBytes {
				return innerOffset, err
			}
		}
		return 0, io.EOF
	case filesystem.Hole:
		if offset < r.sizeBytes {
			innerOffset, err := r.base.GetNextRegionOffset(offset, regionType)
			if innerOffset < r.sizeBytes {
				return innerOffset, err
			}
		}
		return max(offset, r.sizeBytes), nil
	default:
		return 0, status.Errorf(codes.InvalidArgument, "unknown region type %d", regionType)
	}
}
