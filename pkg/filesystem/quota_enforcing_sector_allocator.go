package filesystem

import (
	"github.com/buildbarn/bb-remote-execution/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type quotaEnforcingSectorAllocator struct {
	base             SectorAllocator
	sectorsRemaining util.QuotaMetric
}

// NewQuotaEnforcingSectorAllocator creates a SectorAllocator that
// enforces disk quotas. It limits how many sectors may be allocated
// from an underlying SectorAllocator.
func NewQuotaEnforcingSectorAllocator(base SectorAllocator, maximumSectors int64) SectorAllocator {
	return &quotaEnforcingSectorAllocator{
		base:             base,
		sectorsRemaining: util.NewQuotaMetric(maximumSectors),
	}
}

func (a *quotaEnforcingSectorAllocator) AllocateContiguous(maximum int) (uint32, int, error) {
	if !a.sectorsRemaining.Allocate(int64(maximum)) {
		return 0, 0, status.Error(codes.InvalidArgument, "Sector count quota reached")
	}
	first, count, err := a.base.AllocateContiguous(maximum)
	a.sectorsRemaining.Release(int64(maximum - count))
	return first, count, err
}

func (a *quotaEnforcingSectorAllocator) FreeContiguous(first uint32, count int) {
	a.sectorsRemaining.Release(int64(count))
	a.base.FreeContiguous(first, count)
}

func (a *quotaEnforcingSectorAllocator) FreeList(sectors []uint32) {
	count := int64(0)
	for i := range sectors {
		if sectors[i] != 0 {
			count++
		}
	}
	a.sectorsRemaining.Release(count)
	a.base.FreeList(sectors)
}
