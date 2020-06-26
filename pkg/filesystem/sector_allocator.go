package filesystem

// SectorAllocator is used by BlockDeviceBackedFilePool to allocate
// space on the block device that is needed to store files.
type SectorAllocator interface {
	// Allocate a contiguous range of sectors.
	//
	// Under high utilization, it may not be possible to allocate
	// all space contiguously. In that case, this function returns
	// fewer sectors than requested. Repeated calls to this function
	// are necessary to request the desired amount of space, albeit
	// fragmented.
	//
	// Sector numbers handed out by this function start at one.
	// Zero can be used by the user of this interface for special
	// purposes (e.g., sparse files).
	AllocateContiguous(maximum int) (uint32, int, error)
	// Free a contiguous range of sectors. It is invalid to call
	// this function with the first sector number being zero.
	FreeContiguous(first uint32, count int)
	// Free a potentially fragmented list of sectors. Elements with
	// value zero are ignored.
	FreeList(sectors []uint32)
}
