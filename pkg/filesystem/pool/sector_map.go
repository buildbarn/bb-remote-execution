package pool

import (
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SectorMap is a struct which implements mapping from logical to
// physical sectors in a manner based on the unix inode pointer
// structure.
//
// It is a compact struct designed to densely represent the sectors used
// by a file. Since the vast majority of files are small and they tend
// to cluster their data this struct is a good fit for our use cases.
//
// A value of 0 in a direct array (or a nil pointer in an indirect
// array) means that there is no sector allocated for that index.
// Otherwise the value corresponds to the sector index where the data
// can be found.
//
// The larger the file the more indirections it allows. For a typical
// 4096 byte sector size this results in:
//   - No indirection for files up to ~40KiB
//   - Single indirection for files up to ~8MiB
//   - Double indirection for files up to ~16GiB
//   - Triple indirection for files up to ~32TiB
//
// All sector addresses above tripleIndirectionSectors will be treated
// as being unmapped and attempting to map will return an error.
//
// The zero initialization of a sector map is a valid sector map that
// maps no sectors.
type SectorMap struct {
	direct [directSectors]uint32
	single *indirect[uint32]
	double *indirect[*indirect[uint32]]
	triple *indirect[*indirect[*indirect[uint32]]]
}

// indirectionArraySize is the base size of the indirection array.
//
// directSectors, singleIndirectionSectors, doubleIndirectionSectors,
// and tripleIndirectionSectors are the maximum number of sectors which
// can be represented by a maximum of 0, 1, 2, and 3 indirections
// respectively.
const (
	indirectionArraySize     = 2048
	directSectors            = 12
	singleIndirectionSectors = directSectors + indirectionArraySize
	doubleIndirectionSectors = singleIndirectionSectors + indirectionArraySize*indirectionArraySize
	tripleIndirectionSectors = doubleIndirectionSectors + indirectionArraySize*indirectionArraySize*indirectionArraySize
)

type indirect[T any] struct {
	val [indirectionArraySize]T
}

// GetNextMappedSector returns the logical index of the next mapped
// sector in the sector map. If the start index is mapped then start is
// returned. If there are no more mapped sectors then io.EOF is
// returned.
func (sp *SectorMap) GetNextMappedSector(logical uint64) (uint64, error) {
	current := logical
	for {
		offset, list := sp.getNextDirectSectorList(current)
		if list == nil {
			return 0, io.EOF
		}
		for i := range list {
			if list[i] != 0 {
				return current + offset + uint64(i), nil
			}
		}
		next := uint64(current) + uint64(offset) + uint64(len(list))
		if next >= tripleIndirectionSectors {
			return 0, io.EOF
		}
		current = next
	}
}

func firstUnmappedSingle(x uint64, single *indirect[uint32]) (uint64, error) {
	if single == nil {
		return 0, nil
	}
	start := x
	for i := start; i < indirectionArraySize; i++ {
		if single.val[i] == 0 {
			return i, nil
		}
	}
	return 0, io.EOF
}

func firstUnmappedDouble(x uint64, double *indirect[*indirect[uint32]]) (uint64, error) {
	if double == nil {
		return 0, nil
	}
	start := x / indirectionArraySize
	remainder := x % indirectionArraySize
	for i := start; i < indirectionArraySize; i++ {
		if double.val[i] == nil {
			return i*indirectionArraySize + remainder, nil
		}
		offset, err := firstUnmappedSingle(remainder, double.val[i])
		if err == nil {
			return i*indirectionArraySize + offset, nil
		}
		remainder = 0
	}
	return 0, io.EOF
}

func firstUnmappedTriple(x uint64, triple *indirect[*indirect[*indirect[uint32]]]) (uint64, error) {
	if triple == nil {
		return 0, nil
	}
	start := x / (indirectionArraySize * indirectionArraySize)
	remainder := x % (indirectionArraySize * indirectionArraySize)
	for i := start; i < indirectionArraySize; i++ {
		if triple.val[i] == nil {
			return i*indirectionArraySize*indirectionArraySize + remainder, nil
		}
		offset, err := firstUnmappedDouble(remainder, triple.val[i])
		if err == nil {
			return i*indirectionArraySize*indirectionArraySize + offset, nil
		}
		remainder = 0
	}
	return 0, io.EOF
}

// GetNextUnmappedSector returns the logical index of the next unmapped
// sector in the sector map. If the start index is unmapped then start
// is returned.
//
// This implementation of sector map will always have an unmapped sector
// as the sector from 859410596 and above are not mappable.
func (sp *SectorMap) GetNextUnmappedSector(logical uint64) (uint64, error) {
	for i := logical; i < directSectors; i++ {
		if sp.direct[i] == 0 {
			return i, nil
		}
		logical = i + 1
	}
	if logical < singleIndirectionSectors {
		ret, err := firstUnmappedSingle(logical-directSectors, sp.single)
		if err == nil {
			return ret + directSectors, nil
		}
		logical = singleIndirectionSectors
	}
	if logical < doubleIndirectionSectors {
		ret, err := firstUnmappedDouble(logical-singleIndirectionSectors, sp.double)
		if err == nil {
			return ret + singleIndirectionSectors, nil
		}
		logical = doubleIndirectionSectors
	}
	if logical < tripleIndirectionSectors {
		ret, err := firstUnmappedTriple(logical-doubleIndirectionSectors, sp.triple)
		if err == nil {
			return ret + doubleIndirectionSectors, nil
		}
		logical = tripleIndirectionSectors
	}
	// No more mapping are possible for this implementation.
	return logical, nil
}

func getPhysicalIndexSingle(x uint64, single *indirect[uint32]) uint32 {
	if single == nil {
		return 0
	}
	return single.val[x]
}

func getPhysicalIndexDouble(x uint64, double *indirect[*indirect[uint32]]) uint32 {
	if double == nil {
		return 0
	}
	i := x / indirectionArraySize
	j := x % indirectionArraySize
	return getPhysicalIndexSingle(j, double.val[i])
}

func getPhysicalIndexTriple(x uint64, triple *indirect[*indirect[*indirect[uint32]]]) uint32 {
	if triple == nil {
		return 0
	}
	i := x / indirectionArraySize / indirectionArraySize
	j := x % (indirectionArraySize * indirectionArraySize)
	return getPhysicalIndexDouble(j, triple.val[i])
}

// GetPhysicalIndex returns the physical sector index for a given
// logical index. This physical index may be zero which indicates that
// this logical sector is unmapped.
func (sp *SectorMap) GetPhysicalIndex(logical uint64) uint32 {
	if logical < directSectors {
		return sp.direct[logical]
	}
	if logical < singleIndirectionSectors {
		return getPhysicalIndexSingle(logical-directSectors, sp.single)
	}
	if logical < doubleIndirectionSectors {
		return getPhysicalIndexDouble(logical-singleIndirectionSectors, sp.double)
	}
	return getPhysicalIndexTriple(logical-doubleIndirectionSectors, sp.triple)
}

func getNextDirectSectorListFromSingle(x uint64, single *indirect[uint32]) (uint64, []uint32) {
	if single == nil {
		return 0, nil
	}
	return 0, single.val[x:]
}

func getNextDirectSectorListFromDouble(x uint64, double *indirect[*indirect[uint32]]) (uint64, []uint32) {
	if double == nil {
		return 0, nil
	}
	start := x / indirectionArraySize
	remainder := x % indirectionArraySize
	for i := start; i < indirectionArraySize; i++ {
		offset, list := getNextDirectSectorListFromSingle(remainder, double.val[i])
		if list != nil {
			foundPosition := i*indirectionArraySize + remainder + offset
			return foundPosition - x, list
		}
		remainder = 0
	}
	return 0, nil
}

func getNextDirectSectorListFromTriple(x uint64, triple *indirect[*indirect[*indirect[uint32]]]) (uint64, []uint32) {
	if triple == nil {
		return 0, nil
	}
	start := x / (indirectionArraySize * indirectionArraySize)
	remainder := x % (indirectionArraySize * indirectionArraySize)
	for i := start; i < indirectionArraySize; i++ {
		if triple.val[i] != nil {
			offset, list := getNextDirectSectorListFromDouble(remainder, triple.val[i])
			if list != nil {
				foundPosition := i*indirectionArraySize*indirectionArraySize + remainder + offset
				return foundPosition - x, list
			}
		}
		remainder = 0
	}
	return 0, nil
}

// getNextDirectSectorList return the next direct list of physical
// sectors mapped by the sector pointer. It also returns the offset from
// the start value from which this list starts.
//
// If list is nil there is no possible list remaining, if list is non
// nil there may or may not be another list after
// start+offset+len(list).
func (sp *SectorMap) getNextDirectSectorList(start uint64) (uint64, []uint32) {
	if start < directSectors {
		return 0, sp.direct[start:]
	}
	extraOffset := uint64(0)
	if start < singleIndirectionSectors {
		offset, list := getNextDirectSectorListFromSingle(start-directSectors, sp.single)
		if list != nil {
			return offset, list
		}
		extraOffset += singleIndirectionSectors - start
		start = singleIndirectionSectors
	}
	if start < doubleIndirectionSectors {
		offset, list := getNextDirectSectorListFromDouble(start-singleIndirectionSectors, sp.double)
		if list != nil {
			return extraOffset + offset, list
		}
		extraOffset += doubleIndirectionSectors - start
		start = doubleIndirectionSectors
	}

	offset, list := getNextDirectSectorListFromTriple(start-doubleIndirectionSectors, sp.triple)
	if list != nil {
		return extraOffset + offset, list
	}
	return 0, nil
}

// FreeSectors iterates through all direct sector lists starting from a
// given logical offset and applies the provided callback function to
// each list.
func (sp *SectorMap) FreeSectors(start uint64, callback func([]uint32)) {
	current := start
	for {
		offset, list := sp.getNextDirectSectorList(current)
		if list == nil {
			break
		}
		callback(list)
		next := current + offset + uint64(len(list))
		current = next
	}
}

func clearSingle(x uint64, single *indirect[uint32]) bool {
	if single == nil {
		return true
	}
	for i := x; i < indirectionArraySize; i++ {
		single.val[i] = 0
	}
	for i := uint64(0); i < x; i++ {
		if single.val[i] != 0 {
			return false
		}
	}
	return true
}

func clearDouble(x uint64, double *indirect[*indirect[uint32]]) bool {
	if double == nil {
		return true
	}
	start := x / indirectionArraySize
	remainder := x % indirectionArraySize
	for i := start; i < indirectionArraySize; i++ {
		if remainder == 0 || clearSingle(remainder, double.val[i]) {
			double.val[i] = nil
		}
		remainder = 0
	}
	for i := uint64(0); i <= start; i++ {
		if double.val[i] != nil {
			return false
		}
	}
	return true
}

func clearTriple(x uint64, triple *indirect[*indirect[*indirect[uint32]]]) bool {
	if triple == nil {
		return true
	}
	start := x / (indirectionArraySize * indirectionArraySize)
	remainder := x % (indirectionArraySize * indirectionArraySize)
	for i := start; i < indirectionArraySize; i++ {
		if remainder == 0 || clearDouble(remainder, triple.val[i]) {
			triple.val[i] = nil
		}
		remainder = 0
	}
	for i := uint64(0); i <= start; i++ {
		if triple.val[i] != nil {
			return false
		}
	}
	return true
}

// Truncate the sector mapper to a given length. All logical mappings
// above the given length are dropped.
func (sp *SectorMap) Truncate(length uint64) {
	for i := length; i < directSectors; i++ {
		sp.direct[i] = 0
	}
	if length <= directSectors || clearSingle(length-directSectors, sp.single) {
		sp.single = nil
	}
	if length <= singleIndirectionSectors || clearDouble(length-singleIndirectionSectors, sp.double) {
		sp.double = nil
	}
	if length <= doubleIndirectionSectors || clearTriple(length-doubleIndirectionSectors, sp.triple) {
		sp.triple = nil
	}
}

func setPhysicalDouble(double *indirect[*indirect[uint32]], logical uint64, physical uint32) {
	i := logical / indirectionArraySize
	j := logical % indirectionArraySize
	if double.val[i] == nil {
		double.val[i] = &indirect[uint32]{}
	}
	double.val[i].val[j] = physical
}

func setPhysicalTriple(triple *indirect[*indirect[*indirect[uint32]]], logical uint64, physical uint32) {
	i := logical / (indirectionArraySize * indirectionArraySize)
	j := logical % (indirectionArraySize * indirectionArraySize)
	if triple.val[i] == nil {
		triple.val[i] = &indirect[*indirect[uint32]]{}
	}
	setPhysicalDouble(triple.val[i], j, physical)
}

func (sp *SectorMap) setPhysicalIndex(logical uint64, physical uint32) {
	if logical < directSectors {
		sp.direct[logical] = physical
	} else if logical < singleIndirectionSectors {
		if sp.single == nil {
			sp.single = &indirect[uint32]{}
		}
		sp.single.val[logical-directSectors] = physical
	} else if logical < doubleIndirectionSectors {
		if sp.double == nil {
			sp.double = &indirect[*indirect[uint32]]{}
		}
		setPhysicalDouble(sp.double, logical-singleIndirectionSectors, physical)
	} else {
		if sp.triple == nil {
			sp.triple = &indirect[*indirect[*indirect[uint32]]]{}
		}
		setPhysicalTriple(sp.triple, logical-doubleIndirectionSectors, physical)
	}
}

// InsertSectorsContiguous inserts a contigous range of sectors into the
// sector mapper. Attempting to overwrite an already mapped sector with
// this method is an error. I.e. the entire logical range should be
// unmapped.
//
// In case this method returns an error it guarantees that the internal
// state is unchanged.
func (sp *SectorMap) InsertSectorsContiguous(logical uint64, physical, length uint32) error {
	if math.MaxUint64-uint64(length) < logical {
		return status.Errorf(codes.Internal, "Attempted to insert %d sectors from logical index %d but this would overflow", length, logical)
	}
	if uint64(logical)+uint64(length) > tripleIndirectionSectors {
		return status.Errorf(codes.Internal, "Attempted to insert %d sectors from logical index %d but the sector map can only map indexes less than %d", length, logical, uint64(tripleIndirectionSectors))
	}
	if uint64(physical)+uint64(length) > math.MaxUint32+1 {
		return status.Errorf(codes.Internal, "Attempted to insert %d sectors from physical index %d but this would overflow", length, physical)
	}

	for i := range length {
		current := logical + uint64(i)
		val := sp.GetPhysicalIndex(current)
		if val != 0 {
			// Roll back partial insertion.
			for j := range i {
				sp.setPhysicalIndex(logical+uint64(j), 0)
			}
			return status.Errorf(codes.Internal, "Attempted to insert a sector at logical adress %d but it is already occupied by %d", current, val)
		}
		sp.setPhysicalIndex(current, physical+i)
	}
	return nil
}
