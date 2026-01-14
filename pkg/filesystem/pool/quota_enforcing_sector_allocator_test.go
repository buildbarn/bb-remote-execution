package pool_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

// testRemainingSectorQuota is a helper function for the
// QuotaEnforcingSectorAllocator tests to check that a certain amount of
// space is available within the allocator.
func testRemainingSectorQuota(t *testing.T, ctrl *gomock.Controller, underlyingAllocator *mock.MockSectorAllocator, allocator pool.SectorAllocator, sectorsRemaining int64) {
	// Check that the remaining number of sectors are available by
	// allocating all of them.
	sectors := make([]uint32, sectorsRemaining)
	for i := 0; i < int(sectorsRemaining); i++ {
		underlyingAllocator.EXPECT().AllocateContiguous(gomock.Any()).Return(uint32(i+1), 1, nil)
		sector, count, err := allocator.AllocateContiguous(1)
		sectors[i] = sector
		require.NoError(t, err)
		require.Equal(t, count, 1)
	}
	_, _, err := allocator.AllocateContiguous(1)
	require.Equal(t, err, status.Error(codes.InvalidArgument, "Sector count quota reached"))
	underlyingAllocator.EXPECT().FreeList(gomock.Any())
	allocator.FreeList(sectors)
}

func TestQuotaEnforcingSectorAllocator(t *testing.T) {
	ctrl := gomock.NewController(t)

	underlyingAllocator := mock.NewMockSectorAllocator(ctrl)
	allocator := pool.NewQuotaEnforcingSectorAllocator(underlyingAllocator, 1)

	// All of the sectors should be available.
	testRemainingSectorQuota(t, ctrl, underlyingAllocator, allocator, 1)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	filePool := pool.NewBlockDeviceBackedFilePool(
		blockDevice,
		allocator,
		/*sectorSizeBytes=*/ 4,
	)

	f, err := filePool.NewFile(pool.ZeroHoleSource, 0)
	require.NoError(t, err)
	var p []byte = []byte("buffer")

	// File is currently a zero byte file, there is no data to read.
	n, err := f.ReadAt(p[:], 1_000_000)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)

	// Truncate the file to 8 billion bytes, expect no sectors to be
	// allocated.
	err = f.Truncate(8_000_000_000)
	require.NoError(t, err)

	// Write hello to byte position 8 billion, with sector size 4
	// this corresponds to that sector 2 billion will be written to.
	// The value written to the file will be "hi", the value
	// actually written to disk will have zero padding.
	p = []byte("hi\x00\x00")
	underlyingAllocator.EXPECT().AllocateContiguous(1).Return(uint32(1), 1, nil)
	blockDevice.EXPECT().WriteAt(p, int64(0)).Return(4, nil)
	p = []byte("hi")
	n, err = f.WriteAt(p, 8_000_000_000)
	require.Equal(t, 2, n)
	require.NoError(t, err)

	// Try to read 4 bytes every billion bytes, expect to get zeroes
	// from sparseness without touching the blockdevice.
	for i := int64(0); i < 8; i++ {
		p = []byte("data")
		n, err = f.ReadAt(p, i*1_000_000_000)
		require.Equal(t, 4, n)
		require.NoError(t, err)
		require.Equal(t, []byte("\x00\x00\x00\x00"), p)
	}

	// Try to read back 8 bytes of data from position 8 billion,
	// expect to actually read 5 bytes.
	p = []byte("datadata")
	blockDevice.EXPECT().ReadAt(p[:2], int64(0)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			return copy(p, []byte("hi")), nil
		})
	n, err = f.ReadAt(p, 8_000_000_000)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("hitadata"), p)
	require.Equal(t, io.EOF, err)

	// Try to write another byte at the begining of the file, expect
	// an error since we do not have sufficient quota for another sector.
	p = []byte("a")
	n, err = f.WriteAt(p, 0)
	require.Equal(t, 0, n)
	require.Error(t, err)
	require.Equal(t, err, status.Error(codes.InvalidArgument, "Sector count quota reached"))

	// Truncate the file to half it's size, expect the sector quota
	// to be restored.
	underlyingAllocator.EXPECT().FreeList(gomock.Any())
	err = f.Truncate(4_000_000_000)
	require.NoError(t, err)
	testRemainingSectorQuota(t, ctrl, underlyingAllocator, allocator, 1)

	// Writing to a sector at the beginning of the file should now
	// be possible.
	underlyingAllocator.EXPECT().AllocateContiguous(1).Return(uint32(1), 1, nil)
	blockDevice.EXPECT().WriteAt([]byte("a\x00\x00\x00"), int64(0)).Return(4, nil)
	p = []byte("a")
	n, err = f.WriteAt(p, 0)
	require.Equal(t, 1, n)
	require.NoError(t, err)
}
