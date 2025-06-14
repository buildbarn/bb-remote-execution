package pool

// SectorMapper is an interface that provides a mapping from logical to
// physical sectors.
type SectorMapper interface {
	// GetNextDirectSectorList returns a direct list of physical
	// sectors mapped by the sector mapper. It also returns the
	// logical index of the next possible mapped sectors. If no such
	// lists exist then the function returns an empty list.
	GetNextDirectSectorList(start uint32) (uint32, []uint32)

	// GetPhysicalIndex returns the physical sector index for a
	// given logical index. This physical index may be zero which
	// indicates that this logical sector is unmapped.
	GetPhysicalIndex(index uint32) uint32

	// GetNextUnmappedSector returns the logical index of the next
	// unmapped sector in the sector map. If the start index is
	// unmapped then start is returned. If there are no more
	// unmapped sectors then io.EOF is returned.
	GetNextUnmappedSector(start uint32) (uint32, error)

	// GetNextMappedSector returns the logical index of the next
	// mapped sector in the sector map. If the start index is mapped
	// then start is returned. If there are no more mapped sectors
	// then io.EOF is returned.
	GetNextMappedSector(start uint32) (uint32, error)

	// Returns the logical size of the sector mapper based on the
	// last mapped sector. All sectors indices greater than or equal
	// to this length are unmapped.
	GetLogicalSize() uint32

	// Truncate the sector mapper to a given length. All logical
	// mappings above the given length are dropped.
	Truncate(length uint32)

	// Inserts a contigous range of sectors into the sector mapper.
	// Attempting to overwrite an already mapped sector with this
	// method is an error. I.e. the entire logical range should be
	// unmapped.
	InsertSectorsContiguous(logical uint32, physical uint32, length uint32) error
}
