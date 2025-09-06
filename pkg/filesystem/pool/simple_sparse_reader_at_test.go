package pool_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSimpleSparseReaderAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	readerAt := mock.NewMockReaderAt(ctrl)
	t.Run("TestGetNextRegionOffset", func(t *testing.T) {
		// All permutations of holes in a 4 character string. Represented
		// by the bitmask of holes (values 0b0000 to 0b1111).
		for i := range 1 << 4 {
			mask := (^uint16(0) >> 4 << 4) | uint16(i)
			isHole := func(index int) bool {
				return mask&(1<<index) != 0
			}
			testHoles := make([]pool.Range, 0, 4)
			for j := range 4 {
				if isHole(j) {
					testHoles = append(testHoles, pool.Range{Off: int64(j), Len: 1})
				}
			}
			s, err := pool.NewSimpleSparseReaderAt(readerAt, testHoles, 4)
			require.NoError(t, err)
			for j := range 4 {
				hole, err := s.GetNextRegionOffset(int64(j), filesystem.Hole)
				// GetNextRegionOffset can return offsets less than or
				// equal to the size of the buffer when searching for
				// holes.
				require.LessOrEqual(t, int(hole), 4, "expected hole to be within bounds for mask %x", mask)
				// There should always be a hole due to the implicit
				// hole at the end of file.
				require.NoError(t, err)
				for k := j; k < int(hole); k++ {
					require.Equal(t, isHole(k), false, "expected no hole at %d", k)
				}
				// The result should be the next hole.
				require.Equal(t, isHole(int(hole)), true, "expected hole at index %d for mask %x", hole, mask)
			}
			for j := range 4 {
				data, err := s.GetNextRegionOffset(int64(j), filesystem.Data)
				// GetNextRegionOffset can return offsets less than the
				// size of the buffer when searching for data.
				require.Less(t, int(data), 4, "expected data to be within bounds for mask %x", mask)
				// If we get io.EOF verify there really is no more data
				// in the sparse reader at.
				if err == io.EOF {
					for k := j; k < 4; k++ {
						require.Equal(t, isHole(k), true)
					}
				} else {
					for k := j; k < int(data); k++ {
						require.Equal(t, isHole(k), true)
					}
					require.Equal(t, isHole(int(data)), false, "expected data at index %d for mask %x", data, mask)
				}
			}
		}
	})

	t.Run("TestReadAt", func(t *testing.T) {
		readerAt.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(4, nil)
		s, err := pool.NewSimpleSparseReaderAt(readerAt, nil, 4)
		require.NoError(t, err)
		n, err := s.ReadAt(make([]byte, 4), 0)
		require.NoError(t, err)
		require.Equal(t, n, 4)
	})
}
