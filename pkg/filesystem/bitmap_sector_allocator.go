package filesystem

import (
	"fmt"
	"math/bits"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bitmapSectorAllocator struct {
	lock       sync.Mutex
	freeBitmap []uint64 // One bits indicate sectors that are free.
	nextSector uint32
}

const (
	allBits = ^uint64(0)
)

// NewBitmapSectorAllocator creates a SectorAllocator that stores
// information on which sectors are allocated in a bitmap. Sectors are
// allocated by sequentially scanning the bitmap, continuing where
// previous calls left off.
//
// Due to its simplicity, this allocator would be prone to heavy
// fragmentation if used for general purpose file systems. For FilePool
// this is not a problem, because files have short lifetimes.
// Fragmentation disappears entirely when a worker goes idle, causing
// the FilePool to go empty.
func NewBitmapSectorAllocator(sectorCount uint32) SectorAllocator {
	// Construct a bitmap. Make the bitmap a bit too big, so that
	// it's always terminated with one or more sectors that are
	// permanently in use. This prevents the need for explicit
	// bounds checking inside our algorithms.
	sa := &bitmapSectorAllocator{
		freeBitmap: make([]uint64, sectorCount/64+1),
	}

	// Mark the exact number of sectors as being free.
	for i := uint32(0); i < sectorCount/64; i++ {
		sa.freeBitmap[i] = allBits
	}
	sa.freeBitmap[sectorCount/64] = ^(allBits << (sectorCount % 64))
	return sa
}

func (sa *bitmapSectorAllocator) AllocateContiguous(maximum int) (uint32, int, error) {
	sa.lock.Lock()
	defer sa.lock.Unlock()

	// Allocate sectors from the current bitmap word.
	split := sa.nextSector / 64
	if m := sa.freeBitmap[split] & (allBits << (sa.nextSector % 64)); m != 0 {
		return sa.allocateAt(split, m, maximum)
	}

	// Allocate sectors from the current location to the end.
	for i := split + 1; i < uint32(len(sa.freeBitmap)); i++ {
		if m := sa.freeBitmap[i]; m != 0 {
			return sa.allocateAt(i, m, maximum)
		}
	}

	// Allocate sectors from the beginning to the current location.
	for i := uint32(0); i <= split; i++ {
		if m := sa.freeBitmap[i]; m != 0 {
			return sa.allocateAt(i, m, maximum)
		}
	}
	return 0, 0, status.Error(codes.ResourceExhausted, "No free sectors available")
}

func (sa *bitmapSectorAllocator) allocateAt(index uint32, mask uint64, maximum int) (uint32, int, error) {
	// Compute the first sector at which to start allocating.
	initialShift := bits.TrailingZeros64(mask)
	firstSector := index*64 + uint32(initialShift)

	// Allocate sectors from the first bitmap word.
	allocated := bits.TrailingZeros64(^(mask >> initialShift))
	if allocated > maximum {
		allocated = maximum
	}
	sa.freeBitmap[index] &^= ^(allBits << allocated) << initialShift

	if initialShift+allocated == 64 {
		// More sectors are requested than available in the
		// first bitmap word. Fully allocate as many bitmap
		// words as possible.
		index++
		maximum -= allocated
		for maximum >= 64 && sa.freeBitmap[index] == allBits {
			sa.freeBitmap[index] = 0
			index++
			maximum -= 64
			allocated += 64
		}

		// Allocate remaining sectors from a final bitmap word.
		available := bits.TrailingZeros64(^sa.freeBitmap[index])
		if available > maximum {
			available = maximum
		}
		sa.freeBitmap[index] &= allBits << available
		allocated += available
	}

	sa.nextSector = firstSector + uint32(allocated)
	return firstSector + 1, allocated, nil
}

func (sa *bitmapSectorAllocator) freeWithMask(index int, mask uint64) {
	if m := sa.freeBitmap[index] & mask; m != 0 {
		panic(fmt.Sprintf("Attempted to free sectors %x at index %d, even though they are not allocated", m, index))
	}
	sa.freeBitmap[index] |= mask
}

func (sa *bitmapSectorAllocator) FreeContiguous(firstSector uint32, count int) {
	firstSector--

	sa.lock.Lock()
	defer sa.lock.Unlock()

	// Free sectors from the initial bitmap word.
	mask := allBits
	if count < 64 {
		mask = ^(allBits << count)
	}
	offsetWithinFirstWord := int(firstSector % 64)
	index := int(firstSector / 64)
	sa.freeWithMask(index, mask<<offsetWithinFirstWord)

	if alreadyFreed := 64 - offsetWithinFirstWord; count > alreadyFreed {
		// More sectors are freed than available in the first
		// bitmap word. Full free as many bitmap words as
		// possible.
		count -= alreadyFreed
		index++
		for count >= 64 {
			if sa.freeBitmap[index] != 0 {
				panic(fmt.Sprintf("Attempted to free sectors index %d, even though they are not allocated", index))
			}
			sa.freeBitmap[index] = allBits
			index++
			count -= 64
		}

		// Free remaining sectors from the final bitmap word.
		sa.freeWithMask(index, ^(allBits << count))
	}
}

func (sa *bitmapSectorAllocator) FreeList(sectors []uint32) {
	sa.lock.Lock()
	defer sa.lock.Unlock()

	for _, sector := range sectors {
		if sector != 0 {
			sector--
			i := sector / 64
			b := sector % 64
			if sa.freeBitmap[i]&(1<<b) != 0 {
				panic(fmt.Sprintf("Attempted to free sector %d, even though it's not allocated", sector))
			}
			sa.freeBitmap[i] |= 1 << b
		}
	}
}
