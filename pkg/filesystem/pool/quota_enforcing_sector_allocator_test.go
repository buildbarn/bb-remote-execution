package pool_test

import (
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
// space is available within the allocator..
func testRemainingSectorQuota(t *testing.T, ctrl *gomock.Controller, underlyingAllocator *mock.MockSectorAllocator, allocator pool.SectorAllocator, sectorsRemaining int64) {
	// Check that the remaining number of sectors are available by
	// allocating all of them.
	sectors := make([]uint32, sectorsRemaining)
	for i := 0; i < int(sectorsRemaining); i++ {
		underlyingAllocator.EXPECT().AllocateContiguous(gomock.Any()).Return(uint32(i), 1, nil)
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
	allocator := pool.NewQuotaEnforcingSectorAllocator(underlyingAllocator, 100)

	// All of the sectors should be available.
	testRemainingSectorQuota(t, ctrl, underlyingAllocator, allocator, 100)

}
