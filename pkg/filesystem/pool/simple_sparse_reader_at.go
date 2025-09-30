package pool

import (
	"io"
	"sort"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReaderAt is type alias of io.ReaderAt used for mock generation.
type ReaderAt = io.ReaderAt

// Range describes an offset interval. Used by SimpleSparseReaderAt to
// describe the holes in the io.ReaderAt.
type Range struct {
	Off int64
	Len int64
}

// NewSimpleSparseReaderAt creates a SparseReaderAt from an io.ReaderAt
// and a list of holes.
//
// Note: This decorates io.ReaderAt with the necessary metadata to
// fulfill the SparseReaderAt interface. It doesn't perform the zeroing
// of reads in holes or padding of reads past the EOF boundary. It is
// assumed that the underlying io.ReaderAt already does that.
func NewSimpleSparseReaderAt(reader io.ReaderAt, holes []Range, sizeBytes int64) (SparseReaderAt, error) {
	sort.Slice(holes, func(i, j int) bool {
		return holes[i].Off < holes[j].Off
	})
	for _, hole := range holes {
		if hole.Len < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "invalid hole: %v", hole)
		}
		if hole.Off > sizeBytes {
			return nil, status.Errorf(codes.InvalidArgument, "hole out of bounds: %v", hole)
		}
	}
	// Explicitly add the implicit hole at the end of file to simplify
	// the code.
	holes = append(holes, Range{Off: sizeBytes, Len: 1})
	// Reduce to simplest reprentation by merging adjacent holes.
	reducedHoles := make([]Range, 0, len(holes))
	prev := holes[0]
	for i := 1; i < len(holes); i++ {
		hole := holes[i]
		prevEnd := prev.Off + prev.Len
		// This hole starts before or is adjacent to the previous hole.
		if hole.Off <= prevEnd {
			end := hole.Off + hole.Len
			// Extend the previous hole to the end of this hole.
			if end > prevEnd {
				prev.Len += end - prevEnd
			}
			continue
		}
		// Save it in the reduced set only if it is not a zero length
		// hole.
		if prev.Len > 0 {
			reducedHoles = append(reducedHoles, prev)
		}
		prev = hole
	}
	reducedHoles = append(reducedHoles, prev)
	return &sparseReaderAt{
		reader:    reader,
		holes:     reducedHoles,
		sizeBytes: sizeBytes,
	}, nil
}

type sparseReaderAt struct {
	reader    io.ReaderAt
	holes     []Range
	sizeBytes int64
}

func (s *sparseReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := s.reader.ReadAt(p, off)
	return n, err
}

func (s *sparseReaderAt) GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error) {
	// Find the offset of the first hole or data >= offset. Resolves it
	// by binary searching for the two surrounding holes. Since we
	// add the implicit hole at the end of file to the list of holes in
	// the constructor we will always have atleast one hole in the list.
	if offset < 0 {
		return 0, status.Errorf(codes.InvalidArgument, "negative offset: %d", offset)
	}
	// Out of bounds.
	if offset >= s.sizeBytes {
		return 0, io.EOF
	}
	// Index of first hole with offset greater than the given offset.
	nextIndex := sort.Search(len(s.holes), func(i int) bool {
		return s.holes[i].Off > offset
	})
	// Index of the last hole with offset less than or equal to the
	// given offset.
	prevIndex := nextIndex - 1
	switch regionType {
	case filesystem.Hole:
		if prevIndex == -1 {
			return s.holes[0].Off, nil
		}
		prev := s.holes[prevIndex]
		if prev.Off+prev.Len > offset {
			return offset, nil
		}
		return s.holes[nextIndex].Off, nil
	case filesystem.Data:
		if prevIndex == -1 {
			return offset, nil
		}
		prev := s.holes[prevIndex]
		after := prev.Off + prev.Len
		if after >= s.sizeBytes {
			return 0, io.EOF
		}
		if after > offset {
			return after, nil
		}
		return offset, nil
	default:
		return 0, status.Errorf(codes.InvalidArgument, "unknown region type: %v", regionType)
	}
}
