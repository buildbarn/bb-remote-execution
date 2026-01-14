package pool_test

import (
	"io"
	"math"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestBlockDeviceBackedFilePool(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	sectorAllocator := mock.NewMockSectorAllocator(ctrl)
	filePool := pool.NewBlockDeviceBackedFilePool(blockDevice, sectorAllocator, 16)
	freedSectors := make([]uint32, 0, 16)
	sectorAllocator.EXPECT().FreeList(gomock.Any()).DoAndReturn(func(sectors []uint32) {
		toFree := make([]uint32, 0, len(sectors))
		for _, s := range sectors {
			if s != 0 {
				freedSectors = append(freedSectors, s)
			}
		}
		freedSectors = append(freedSectors, toFree...)
	}).AnyTimes()
	sectorAllocator.EXPECT().FreeContiguous(gomock.Any(), gomock.Any()).DoAndReturn(func(firstSector uint32, count int) {
		for i := range uint32(count) {
			freedSectors = append(freedSectors, firstSector+i)
		}
	}).AnyTimes()

	t.Run("ReadEmptyFile", func(t *testing.T) {
		// Test that reads on an empty file work as expected.
		freedSectors = freedSectors[:0]
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		require.NoError(t, err)

		var p [10]byte
		n, err := f.ReadAt(p[:], math.MinInt64)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -9223372036854775808"), err)

		n, err = f.ReadAt(p[:], -1)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -1"), err)

		n, err = f.ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		n, err = f.ReadAt(p[:], 1)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		n, err = f.ReadAt(p[:], math.MaxInt64)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		holeSource.EXPECT().Close()
		require.Equal(t, []uint32{}, freedSectors)
		require.NoError(t, f.Close())
	})

	t.Run("Truncate", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		freedSectors = freedSectors[:0]
		require.NoError(t, err)

		// Invalid size.
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative truncation size: -9223372036854775808"), f.Truncate(-9223372036854775808))
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative truncation size: -1"), f.Truncate(-1))

		// Growing and shrinking an empty file should not
		// cause any I/O, as it contains no used sectors.
		require.NoError(t, f.Truncate(16*1024*1024*1024))
		holeSource.EXPECT().Truncate(int64(0))
		require.NoError(t, f.Truncate(0))

		// Add some contents to the file to perform further
		// testing.
		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(5), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(10), int64(80)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		holeSource.EXPECT().ReadAt(gomock.Len(3), int64(93)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Foo\x00\x00\x00"), int64(64)).Return(16, nil)
		n, err := f.WriteAt([]byte("Foo"), 90)
		require.Equal(t, 3, n)
		require.NoError(t, err)

		// Growing the file should not cause any I/O.
		require.NoError(t, f.Truncate(100))

		// Shrinking the file should cause trailing bytes in the
		// last sector to be zeroed. Simulate the case where
		// this fails. The file size should remain as is.
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(69)).
			Return(0, status.Error(codes.Internal, "Disk on fire"))
		require.Equal(t, status.Error(codes.Internal, "Disk on fire"), f.Truncate(85))

		// Perform truncations that do succeed.
		holeSource.EXPECT().Truncate(int64(96))
		require.NoError(t, f.Truncate(96))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00"), int64(76)).Return(16, nil)
		holeSource.EXPECT().Truncate(int64(92))
		require.NoError(t, f.Truncate(92))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00"), int64(74)).Return(16, nil)
		holeSource.EXPECT().Truncate(int64(90))
		require.NoError(t, f.Truncate(90))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(65)).Return(16, nil)
		holeSource.EXPECT().Truncate(int64(81))
		require.NoError(t, f.Truncate(81))

		// Continuing to shrink the file should eventually cause
		// the final sector to be released.
		holeSource.EXPECT().Truncate(int64(80))
		require.NoError(t, f.Truncate(80))
		require.Equal(t, []uint32{5}, freedSectors)

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
	})

	t.Run("WritesAndReadOnSingleSector", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		freedSectors = freedSectors[:0]
		require.NoError(t, err)

		// The initial write to a sector should cause the full
		// sector to be written. This ensures that no
		// unnecessary reads are triggered against storage and
		// that any leading bytes are zeroed.
		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(12), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(2), int64(0)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		holeSource.EXPECT().ReadAt(gomock.Len(9), int64(7)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00Hello\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(176)).Return(16, nil)
		n, err := f.WriteAt([]byte("Hello"), 2)
		require.Equal(t, 5, n)
		require.NoError(t, err)

		// Successive writes to the same sector should not add
		// any null byte padding. All of that work was already
		// done by the previous write operation.
		blockDevice.EXPECT().WriteAt([]byte("world"), int64(184)).Return(5, nil)
		n, err = f.WriteAt([]byte("world"), 8)
		require.Equal(t, 5, n)
		require.NoError(t, err)

		// Reads should be limited to the end-of-file.
		blockDevice.EXPECT().ReadAt(gomock.Len(13), int64(176)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				return copy(p, []byte("\x00\x00Hello\x00world")), nil
			})
		var buf [16]byte
		n, err = f.ReadAt(buf[:], 0)
		require.Equal(t, 13, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("\x00\x00Hello\x00world"), buf[:n])

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
		require.Equal(t, []uint32{12}, freedSectors)
	})

	t.Run("WriteFragmentation", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		freedSectors = freedSectors[:0]
		require.NoError(t, err)

		// Simulate the case where 137 bytes of data needs to be
		// written, requiring 10 sectors of storage space. The
		// sector allocator is not able to give us 10 contiguous
		// sectors, meaning multiple allocations of smaller
		// regions are performed.
		sectorAllocator.EXPECT().AllocateContiguous(10).Return(uint32(75), 3, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(10), int64(32)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)
		blockDevice.EXPECT().WriteAt([]byte("ipsum dolor sit amet, consectetu"), int64(1200)).Return(32, nil)

		sectorAllocator.EXPECT().AllocateContiguous(7).Return(uint32(21), 4, nil)
		blockDevice.EXPECT().WriteAt([]byte("r adipiscing elit. Suspendisse quis mollis eros, sit amet pellen"), int64(320)).Return(64, nil)

		sectorAllocator.EXPECT().AllocateContiguous(3).Return(uint32(105), 2, nil)
		blockDevice.EXPECT().WriteAt([]byte("tesque lectus. Quisque non ex ni"), int64(1664)).Return(32, nil)

		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(40), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(13), int64(179)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("sl.\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(624)).Return(16, nil)

		n, err := f.WriteAt([]byte(
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. "+
				"Suspendisse quis mollis eros, sit amet pellentesque lectus. "+
				"Quisque non ex nisl."), 42)
		require.Equal(t, 137, n)
		require.NoError(t, err)

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
		require.Equal(t, []uint32{75, 76, 77, 21, 22, 23, 24, 105, 106, 40}, freedSectors)
	})

	t.Run("WriteSectorAllocatorFailure", func(t *testing.T) {
		freedSectors = freedSectors[:0]
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		require.NoError(t, err)

		// Failure to allocate sectors should cause the write to
		// fail as well. Any previously allocated sectors should
		// still be attached to the file and freed later on.
		sectorAllocator.EXPECT().AllocateContiguous(5).Return(uint32(75), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(10), int64(32)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)

		sectorAllocator.EXPECT().AllocateContiguous(4).Return(uint32(0), 0, status.Error(codes.ResourceExhausted, "Out of storage space"))

		n, err := f.WriteAt([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."), 42)
		require.Equal(t, 6, n)
		require.Equal(t, status.Error(codes.ResourceExhausted, "Out of storage space"), err)

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
		require.Equal(t, []uint32{75}, freedSectors)
	})

	t.Run("WriteIOFailure", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		freedSectors = freedSectors[:0]
		require.NoError(t, err)

		// Write failures to freshly allocator sectors should
		// cause them to not be attached to the file. The
		// sectors should be released immediately.
		sectorAllocator.EXPECT().AllocateContiguous(5).Return(uint32(75), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(10), int64(32)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)

		sectorAllocator.EXPECT().AllocateContiguous(4).Return(uint32(39), 1, nil)
		blockDevice.EXPECT().WriteAt([]byte("ipsum dolor sit "), int64(608)).Return(0, status.Error(codes.Internal, "Disk failure"))

		n, err := f.WriteAt([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."), 42)
		require.Equal(t, 6, n)
		require.Equal(t, status.Error(codes.Internal, "Disk failure"), err)

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
		require.Equal(t, []uint32{39, 75}, freedSectors)
	})

	t.Run("GetNextRegionOffset", func(t *testing.T) {
		// Test the behavior on empty files.
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		freedSectors = freedSectors[:0]
		require.NoError(t, err)

		_, err = f.GetNextRegionOffset(-1, filesystem.Data)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative seek offset: -1"), err)
		_, err = f.GetNextRegionOffset(-1, filesystem.Hole)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative seek offset: -1"), err)

		_, err = f.GetNextRegionOffset(0, filesystem.Data)
		require.Equal(t, io.EOF, err)
		_, err = f.GetNextRegionOffset(0, filesystem.Hole)
		require.Equal(t, io.EOF, err)

		_, err = f.GetNextRegionOffset(1, filesystem.Data)
		require.Equal(t, io.EOF, err)
		_, err = f.GetNextRegionOffset(1, filesystem.Hole)
		require.Equal(t, io.EOF, err)

		// Test the behavior on a sparse file that starts with a
		// hole and ends with data.
		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(5), 1, nil)
		holeSource.EXPECT().ReadAt(gomock.Len(11), int64(133)).
			DoAndReturn(func(p []byte, off int64) (int, error) {
				clear(p)
				return len(p), nil
			})
		blockDevice.EXPECT().WriteAt([]byte("Hello\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(64)).Return(16, nil)
		n, err := f.WriteAt([]byte("Hello"), 128)
		require.Equal(t, 5, n)
		require.NoError(t, err)

		holeSource.EXPECT().GetNextRegionOffset(int64(0), filesystem.Data).Return(int64(0), io.EOF)
		nextOffset, err := f.GetNextRegionOffset(0, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(0), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(0, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(0), nextOffset)

		holeSource.EXPECT().GetNextRegionOffset(int64(1), filesystem.Data).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(1, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(1), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(1, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(1), nextOffset)

		holeSource.EXPECT().GetNextRegionOffset(int64(128-1), filesystem.Data).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(128-1, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(128-1), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(128-1, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(128-1), nextOffset)

		nextOffset, err = f.GetNextRegionOffset(128, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(128), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(128, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(128+5), nextOffset)

		nextOffset, err = f.GetNextRegionOffset(128+4, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128+4), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(128+4), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(128+4, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(128+5), nextOffset)

		_, err = f.GetNextRegionOffset(128+5, filesystem.Data)
		require.Equal(t, io.EOF, err)
		_, err = f.GetNextRegionOffset(128+5, filesystem.Hole)
		require.Equal(t, io.EOF, err)

		// Test the behavior on a sparse file that ends with a hole.
		require.NoError(t, f.Truncate(384))

		nextOffset, err = f.GetNextRegionOffset(128, filesystem.Data)
		require.NoError(t, err)
		require.Equal(t, int64(128), nextOffset)
		holeSource.EXPECT().GetNextRegionOffset(int64(128), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(128, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(128+16), nextOffset)

		holeSource.EXPECT().GetNextRegionOffset(int64(256), filesystem.Data).Return(int64(0), io.EOF)
		_, err = f.GetNextRegionOffset(256, filesystem.Data)
		require.Equal(t, io.EOF, err)
		holeSource.EXPECT().GetNextRegionOffset(int64(256), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(256, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(256), nextOffset)

		holeSource.EXPECT().GetNextRegionOffset(int64(384-1), filesystem.Data).Return(int64(0), io.EOF)
		_, err = f.GetNextRegionOffset(384-1, filesystem.Data)
		require.Equal(t, io.EOF, err)
		holeSource.EXPECT().GetNextRegionOffset(int64(384-1), filesystem.Hole).Return(int64(0), io.EOF)
		nextOffset, err = f.GetNextRegionOffset(384-1, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(384-1), nextOffset)

		_, err = f.GetNextRegionOffset(384, filesystem.Data)
		require.Equal(t, io.EOF, err)
		_, err = f.GetNextRegionOffset(384, filesystem.Hole)
		require.Equal(t, io.EOF, err)

		holeSource.EXPECT().Close()
		require.NoError(t, f.Close())
		require.Equal(t, []uint32{5}, freedSectors)
	})

	t.Run("WriteAt", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, 0)
		require.NoError(t, err)

		_, err = f.WriteAt([]byte{0}, -1)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Negative write offset: -1"), err)
	})

	t.Run("VeryLargeFile", func(t *testing.T) {
		holeSource := mock.NewMockHoleSource(ctrl)
		f, err := filePool.NewFile(holeSource, math.MaxInt64)
		require.NoError(t, err)
		freedSectors = freedSectors[:0]
		// WriteAt
		sectorAllocator.EXPECT().AllocateContiguous(2).Return(uint32(7), 2, nil)
		holeSource.EXPECT().ReadAt(make([]byte, 15), int64(math.MaxInt64-31)).Return(15, nil)
		holeSource.EXPECT().ReadAt(make([]byte, 12), int64(math.MaxInt64-11)).Return(12, nil)
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00H"), int64(16*(7-1))).Return(16, nil)
		blockDevice.EXPECT().WriteAt([]byte("ello\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(16*(8-1))).Return(16, nil)
		n, err := f.WriteAt([]byte("Hello"), math.MaxInt64-16)
		require.Equal(t, 0, n)
		require.Error(t, err)
		require.Equal(t, []uint32{7, 8}, freedSectors)
		// GetNextRegionOffset for hole
		holeSource.EXPECT().GetNextRegionOffset(int64(math.MaxInt64-1), filesystem.Hole).Return(int64(0), io.EOF)
		offset, err := f.GetNextRegionOffset(math.MaxInt64-1, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(math.MaxInt64-1), offset)
		// GetNextRegionOffset for data
		holeSource.EXPECT().GetNextRegionOffset(int64(math.MaxInt64-1), filesystem.Data).Return(int64(0), io.EOF)
		offset, err = f.GetNextRegionOffset(math.MaxInt64-1, filesystem.Data)
		require.Equal(t, io.EOF, err)
		require.Equal(t, int64(0), offset)
		// ReadAt
		buffer := make([]byte, 10)
		holeSource.EXPECT().ReadAt(make([]byte, 10), int64(math.MaxInt64-11)).Return(10, nil)
		n, err = f.ReadAt(buffer, math.MaxInt64-11)
		require.Equal(t, 10, n)
		require.NoError(t, err)
		require.Equal(t, make([]byte, 10), buffer)
		// Truncate
		holeSource.EXPECT().Truncate(int64(math.MaxInt64 - 10)).Return(nil)
		err = f.Truncate(math.MaxInt64 - 10)
		require.NoError(t, err)
	})
}
