package filesystem_test

import (
	"testing"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBitmapSectorAllocatorExample(t *testing.T) {
	sectorAllocator := re_filesystem.NewBitmapSectorAllocator(1000)

	// Allocate five regions of sectors that span all of storage.
	for i := 0; i < 5; i++ {
		firstSector, sectorCount, err := sectorAllocator.AllocateContiguous(200)
		require.NoError(t, err)
		require.Equal(t, uint32(200*i+1), firstSector)
		require.Equal(t, 200, sectorCount)
	}

	// Allocating successive sectors should fail.
	_, _, err := sectorAllocator.AllocateContiguous(123)
	require.Equal(t, status.Error(codes.ResourceExhausted, "No free sectors available"), err)

	// Free the five regions, bringing us back to the initial state.
	for i := 0; i < 5; i++ {
		sectorAllocator.FreeContiguous(uint32(200*i+1), 200)
	}

	// Allocating a too large number of sectors should now allocate
	// the entire space in one go.
	firstSector, sectorCount, err := sectorAllocator.AllocateContiguous(123456)
	require.NoError(t, err)
	require.Equal(t, uint32(1), firstSector)
	require.Equal(t, 1000, sectorCount)

	// Free up some small holes here and there.
	sectorAllocator.FreeContiguous(83, 12)
	sectorAllocator.FreeContiguous(241, 91)
	sectorAllocator.FreeList([]uint32{503, 1000, 504, 1})

	// Attempt to allocate these holes again. They should be
	// returned in incrementing order.
	for _, a := range []struct {
		firstSector uint32
		sectorCount int
	}{{1, 1}, {83, 12}, {241, 91}, {503, 2}, {1000, 1}} {
		firstSector, sectorCount, err := sectorAllocator.AllocateContiguous(123456)
		require.NoError(t, err)
		require.Equal(t, a.firstSector, firstSector)
		require.Equal(t, a.sectorCount, sectorCount)
	}

	// With all of the holes filled up, successive allocations are
	// no longer possible.
	_, _, err = sectorAllocator.AllocateContiguous(123)
	require.Equal(t, status.Error(codes.ResourceExhausted, "No free sectors available"), err)
}
