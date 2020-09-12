package filesystem_test

import (
	"io"
	"math"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBlockDeviceBackedFilePool(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	sectorAllocator := mock.NewMockSectorAllocator(ctrl)
	pool := re_filesystem.NewBlockDeviceBackedFilePool(blockDevice, sectorAllocator, 16)

	t.Run("ReadEmptyFile", func(t *testing.T) {
		// Test that reads on an empty file work as expected.
		f, err := pool.NewFile()
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

		require.NoError(t, f.Close())
	})

	t.Run("Truncate", func(t *testing.T) {
		f, err := pool.NewFile()
		require.NoError(t, err)

		// Invalid size.
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative truncation size: -9223372036854775808"), f.Truncate(-9223372036854775808))
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative truncation size: -1"), f.Truncate(-1))

		// Growing and shrinking an empty file should not
		// cause any I/O, as it contains no used sectors.
		require.NoError(t, f.Truncate(16*1024*1024*1024))
		require.NoError(t, f.Truncate(0))

		// Add some contents to the file to perform further
		// testing.
		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(5), 1, nil)
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
		require.NoError(t, f.Truncate(96))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00"), int64(76)).Return(16, nil)
		require.NoError(t, f.Truncate(92))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00"), int64(74)).Return(16, nil)
		require.NoError(t, f.Truncate(90))
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(65)).Return(16, nil)
		require.NoError(t, f.Truncate(81))

		// Continuing to shrink the file should eventually cause
		// the final sector to be released.
		sectorAllocator.EXPECT().FreeList([]uint32{5})
		require.NoError(t, f.Truncate(80))

		require.NoError(t, f.Close())
	})

	t.Run("WritesAndReadOnSingleSector", func(t *testing.T) {
		f, err := pool.NewFile()
		require.NoError(t, err)

		// The initial write to a sector should cause the full
		// sector to be written. This ensures that no
		// unnecessary reads are triggered against storage and
		// that any leading bytes are zeroed.
		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(12), 1, nil)
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

		sectorAllocator.EXPECT().FreeList([]uint32{12})
		require.NoError(t, f.Close())
	})

	t.Run("WriteFragmentation", func(t *testing.T) {
		f, err := pool.NewFile()
		require.NoError(t, err)

		// Simulate the case where 137 bytes of data needs to be
		// written, requiring 10 sectors of storage space. The
		// sector allocator is not able to give us 10 contiguous
		// sectors, meaning multiple allocations of smaller
		// regions are performed.
		sectorAllocator.EXPECT().AllocateContiguous(10).Return(uint32(75), 3, nil)
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)
		blockDevice.EXPECT().WriteAt([]byte("ipsum dolor sit amet, consectetu"), int64(1200)).Return(32, nil)

		sectorAllocator.EXPECT().AllocateContiguous(7).Return(uint32(21), 4, nil)
		blockDevice.EXPECT().WriteAt([]byte("r adipiscing elit. Suspendisse quis mollis eros, sit amet pellen"), int64(320)).Return(64, nil)

		sectorAllocator.EXPECT().AllocateContiguous(3).Return(uint32(105), 2, nil)
		blockDevice.EXPECT().WriteAt([]byte("tesque lectus. Quisque non ex ni"), int64(1664)).Return(32, nil)

		sectorAllocator.EXPECT().AllocateContiguous(1).Return(uint32(40), 1, nil)
		blockDevice.EXPECT().WriteAt([]byte("sl.\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), int64(624)).Return(16, nil)

		n, err := f.WriteAt([]byte(
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. "+
				"Suspendisse quis mollis eros, sit amet pellentesque lectus. "+
				"Quisque non ex nisl."), 42)
		require.Equal(t, 137, n)
		require.NoError(t, err)

		sectorAllocator.EXPECT().FreeList([]uint32{0, 0, 75, 76, 77, 21, 22, 23, 24, 105, 106, 40})
		require.NoError(t, f.Close())
	})

	t.Run("WriteSectorAllocatorFailure", func(t *testing.T) {
		f, err := pool.NewFile()
		require.NoError(t, err)

		// Failure to allocate sectors should cause the write to
		// fail as well. Any previously allocated sectors should
		// still be attached to the file and freed later on.
		sectorAllocator.EXPECT().AllocateContiguous(5).Return(uint32(75), 1, nil)
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)

		sectorAllocator.EXPECT().AllocateContiguous(4).Return(uint32(0), 0, status.Error(codes.ResourceExhausted, "Out of storage space"))

		n, err := f.WriteAt([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."), 42)
		require.Equal(t, 6, n)
		require.Equal(t, status.Error(codes.ResourceExhausted, "Out of storage space"), err)

		sectorAllocator.EXPECT().FreeList([]uint32{0, 0, 75})
		require.NoError(t, f.Close())
	})

	t.Run("WriteIOFailure", func(t *testing.T) {
		f, err := pool.NewFile()
		require.NoError(t, err)

		// Write failures to freshly allocator sectors should
		// cause them to not be attached to the file. The
		// sectors should be released immediately.
		sectorAllocator.EXPECT().AllocateContiguous(5).Return(uint32(75), 1, nil)
		blockDevice.EXPECT().WriteAt([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Lorem "), int64(1184)).Return(16, nil)

		sectorAllocator.EXPECT().AllocateContiguous(4).Return(uint32(39), 1, nil)
		blockDevice.EXPECT().WriteAt([]byte("ipsum dolor sit "), int64(608)).Return(0, status.Error(codes.Internal, "Disk failure"))
		sectorAllocator.EXPECT().FreeContiguous(uint32(39), 1)

		n, err := f.WriteAt([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."), 42)
		require.Equal(t, 6, n)
		require.Equal(t, status.Error(codes.Internal, "Disk failure"), err)

		sectorAllocator.EXPECT().FreeList([]uint32{0, 0, 75})
		require.NoError(t, f.Close())
	})
}
