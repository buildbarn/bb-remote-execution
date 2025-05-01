package pool_test

import (
	"io"
	"strings"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/stretchr/testify/require"
)

func TestTruncatableSparseReaderAt_ReadAt(t *testing.T) {
	text := "Hello World!"
	underlyingSize := int64(len(text))
	sparseReader, err := pool.NewSimpleSparseReaderAt(strings.NewReader(text), nil, underlyingSize)
	require.NoError(t, err)

	t.Run("ReadWithinBounds", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)

		buf := make([]byte, 5)
		n, err := r.ReadAt(buf, 0)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte("Hello"), buf[:n])
	})

	t.Run("ReadAcrossBoundary", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)

		buf := make([]byte, 7)
		n, err := r.ReadAt(buf, 6)
		require.Equal(t, err, io.EOF)
		require.Equal(t, 6, n)
		require.Equal(t, []byte("World!\x00"), buf)
	})

	t.Run("TruncateHidesData", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)

		require.NoError(t, r.Truncate(5))
		buf := make([]byte, 6)
		n, err := r.ReadAt(buf, 0)
		require.Equal(t, err, io.EOF)
		require.Equal(t, 5, n)
		require.Equal(t, []byte("Hello\x00"), buf)
	})

	t.Run("ShrinkThenGrow", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)

		require.NoError(t, r.Truncate(2))
		buf := make([]byte, 3)
		n, err := r.ReadAt(buf, 0)
		require.Equal(t, err, io.EOF)
		require.Equal(t, 2, n)
		require.Equal(t, []byte("He\x00"), buf)

		require.NoError(t, r.Truncate(5))
		n, err = r.ReadAt(buf, 0)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte("He\x00"), buf)
	})

	t.Run("EdgeCaseEmptyUnderlying", func(t *testing.T) {
		sparseReader, err := pool.NewSimpleSparseReaderAt(strings.NewReader(""), nil, 0)
		r := pool.NewTruncatableSparseReaderAt(sparseReader, 0)
		require.NoError(t, err)
		buf := make([]byte, 5)
		n, err := r.ReadAt(buf, 0)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
		require.NoError(t, r.Truncate(5))
		n, err = r.ReadAt(buf, 0)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte("\x00\x00\x00\x00\x00"), buf)
	})
}

func TestTruncatableSparseReaderAt_GetNextRegionOffset(t *testing.T) {
	text := "Hell\x00\x00World!"
	underlyingSize := int64(len(text))
	sparseReader, err := pool.NewSimpleSparseReaderAt(strings.NewReader(text), []pool.Range{{Off: 4, Len: 2}}, underlyingSize)
	require.NoError(t, err)

	t.Run("Untruncated", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)
		var err error
		var nextOffset int64
		// Holes should be at [4,5] and special eof hole at 12.
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, int64(4))
		}
		for i := int64(4); i < 6; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		for i := int64(6); i < underlyingSize; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, underlyingSize)
		}
		nextOffset, err = r.GetNextRegionOffset(underlyingSize, filesystem.Hole)
		require.Equal(t, err, io.EOF)
		// Data should be in [0,3] and [6,11]
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		for i := int64(4); i < 6; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.NoError(t, err)
			require.Equal(t, nextOffset, int64(6))
		}
		for i := int64(6); i < underlyingSize; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		nextOffset, err = r.GetNextRegionOffset(underlyingSize, filesystem.Data)
		require.Equal(t, err, io.EOF)
	})

	t.Run("TruncateToHole", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)
		var err error
		var nextOffset int64
		err = r.Truncate(6)
		require.NoError(t, err)
		// Holes should be at [4,5].
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, int64(4))
		}
		for i := int64(4); i < 6; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		nextOffset, err = r.GetNextRegionOffset(6, filesystem.Hole)
		require.Equal(t, err, io.EOF)
		// Data should be at [0,3].
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		for i := int64(4); i < 6; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.Equal(t, io.EOF, err)
		}
		nextOffset, err = r.GetNextRegionOffset(6, filesystem.Data)
		require.Equal(t, io.EOF, err)
	})

	t.Run("TruncateThenGrow", func(t *testing.T) {
		r := pool.NewTruncatableSparseReaderAt(sparseReader, underlyingSize)
		var err error
		var nextOffset int64
		// Truncate down to 4, then grow back up to 12. Expect [4,11] to
		// be holes.
		err = r.Truncate(4)
		require.NoError(t, err)
		err = r.Truncate(12)
		require.NoError(t, err)
		// Holes should be in the region [4,11].
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, int64(4))
		}
		for i := int64(4); i < 12; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Hole)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		nextOffset, err = r.GetNextRegionOffset(12, filesystem.Hole)
		require.Equal(t, err, io.EOF)
		// Data should be in the region [0,3].
		for i := int64(0); i < 4; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.NoError(t, err)
			require.Equal(t, nextOffset, i)
		}
		for i := int64(4); i < 12; i++ {
			nextOffset, err = r.GetNextRegionOffset(i, filesystem.Data)
			require.Equal(t, err, io.EOF)
		}
		nextOffset, err = r.GetNextRegionOffset(12, filesystem.Hole)
		require.Equal(t, err, io.EOF)
	})
}
