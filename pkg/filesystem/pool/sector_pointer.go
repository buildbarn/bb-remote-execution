package pool

import (
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewSectorPointer returns a sector mapper implemented using a
// structure based on the unix inode pointer structure.
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
//   - Single indirection for files up to ~4MiB
//   - Double indirection for files up to ~4GiB
//   - Triple indirection for files up to ~4TiB
func NewSectorPointer() SectorMapper {
	return &sectorPointer{}
}

type sectorPointer struct {
	direct              [12]uint32
	single              *indirect[uint32]
	double              *indirect[*indirect[uint32]]
	triple              *indirect[*indirect[*indirect[uint32]]]
	logicalSectorLength uint32
}

// _SIZE is the base size of the indirection array.
//
// _DIRECT, _SINGLE, _DOUBLE, and _TRIPLE are the maximum number of
// sectors which can be represented by a maximum of 0, 1, 2, and 3
// indirections respectively.
const (
	_SIZE   = 1626 // Smallest size where the full unsigned 32 bit range can be addressed.
	_DIRECT = 12
	_SINGLE = _DIRECT + _SIZE
	_DOUBLE = _SINGLE + _SIZE*_SIZE
	_TRIPLE = _DOUBLE + _SIZE*_SIZE*_SIZE
)

type indirect[T any] struct {
	val [_SIZE]T
}

func (sp *sectorPointer) GetNextMappedSector(logical uint32) (uint32, error) {
	from := logical
	for {
		next, list := sp.GetNextDirectSectorList(from)
		if list == nil {
			return 0, io.EOF
		}
		for i := range list {
			if list[i] != 0 {
				return next - uint32(len(list)-i), nil
			}
		}
		from = next
	}
}

func firstUnmappedSingle(x uint32, single *indirect[uint32]) (uint32, error) {
	if single == nil {
		return 0, nil
	}
	start := x
	for i := start; i < _SIZE; i++ {
		if single.val[i] == 0 {
			return i, nil
		}
	}
	return 0, io.EOF
}

func firstUnmappedDouble(x uint32, double *indirect[*indirect[uint32]]) (uint32, error) {
	if double == nil {
		return 0, nil
	}
	start := x / _SIZE
	remainder := x % _SIZE
	for i := start; i < _SIZE; i++ {
		if double.val[i] == nil {
			return i*_SIZE + remainder, nil
		}
		offset, err := firstUnmappedSingle(remainder, double.val[i])
		if err == nil {
			return i*_SIZE + offset, nil
		}
		remainder = 0
	}
	return 0, io.EOF
}

func noOverflow(x uint64) (uint32, error) {
	if x > math.MaxUint32 {
		return 0, io.EOF
	}
	return uint32(x), nil

}
func firstUnmappedTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) (uint32, error) {
	if triple == nil {
		return 0, nil
	}
	start := x / (_SIZE * _SIZE)
	remainder := x % (_SIZE * _SIZE)
	for i := start; i < _SIZE; i++ {
		if triple.val[i] == nil {
			return noOverflow(uint64(i)*_SIZE*_SIZE + uint64(remainder))
		}
		offset, err := firstUnmappedDouble(remainder, triple.val[i])
		if err == nil {
			return noOverflow(uint64(i)*_SIZE*_SIZE + uint64(offset))
		}
		remainder = 0
	}
	return 0, io.EOF
}

func (sp *sectorPointer) GetNextUnmappedSector(logical uint32) (uint32, error) {
	for i := logical; i < _DIRECT; i++ {
		if sp.direct[i] == 0 {
			return i, nil
		}
		logical = i + 1
	}
	if logical < _SINGLE {
		ret, err := firstUnmappedSingle(logical-_DIRECT, sp.single)
		if err == nil {
			return ret + _DIRECT, nil
		}
		logical = _SINGLE
	}
	if logical < _DOUBLE {
		ret, err := firstUnmappedDouble(logical-_SINGLE, sp.double)
		if err == nil {
			return ret + _SINGLE, nil
		}
		logical = _DOUBLE
	}
	// logical < _TRIPLE
	ret, err := firstUnmappedTriple(logical-_DOUBLE, sp.triple)
	if err == nil {
		return ret + _DOUBLE, nil
	}
	return 0, io.EOF
}

func getPhysicalIndexSingle(x uint32, single *indirect[uint32]) uint32 {
	if single == nil {
		return 0
	}
	return single.val[x]
}

func getPhysicalIndexDouble(x uint32, double *indirect[*indirect[uint32]]) uint32 {
	if double == nil {
		return 0
	}
	i := x / _SIZE
	j := x % _SIZE
	return getPhysicalIndexSingle(j, double.val[i])
}

func getPhysicalIndexTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) uint32 {
	if triple == nil {
		return 0
	}
	i := x / _SIZE / _SIZE
	j := x % _SIZE
	return getPhysicalIndexDouble(j, triple.val[i])
}

func (sp *sectorPointer) GetPhysicalIndex(logical uint32) uint32 {
	if logical < _DIRECT {
		return sp.direct[logical]
	}
	if logical < _SINGLE {
		return getPhysicalIndexSingle(logical-_DIRECT, sp.single)
	}
	if logical < _DOUBLE {
		return getPhysicalIndexDouble(logical-_SINGLE, sp.double)
	}
	return getPhysicalIndexTriple(logical-_DOUBLE, sp.triple)
}

func getNextDirectSectorListFromSingle(x uint32, single *indirect[uint32]) (uint32, []uint32) {
	if single == nil {
		return 0, nil
	}
	return _SIZE, single.val[x:]
}

func getNextDirectSectorListFromDouble(x uint32, double *indirect[*indirect[uint32]]) (uint32, []uint32) {
	if double == nil {
		return 0, nil
	}
	start := x / _SIZE
	remainder := x % _SIZE
	for i := start; i < _SIZE; i++ {
		next, list := getNextDirectSectorListFromSingle(remainder, double.val[i])
		if list != nil {
			return next + i*_SIZE, list
		}
		remainder = 0
	}
	return 0, nil
}

func getNextDirectSectorListFromTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) (uint32, []uint32) {
	if triple == nil {
		return 0, nil
	}
	start := x / (_SIZE * _SIZE)
	remainder := x % (_SIZE * _SIZE)
	for i := start; i < _SIZE; i++ {
		if triple.val[i] != nil {
			next, list := getNextDirectSectorListFromDouble(remainder, triple.val[i])
			if list != nil {
				return next + i*_SIZE*_SIZE, list
			}
		}
		remainder = 0
	}
	return 0, nil
}

func (sp *sectorPointer) GetNextDirectSectorList(start uint32) (uint32, []uint32) {
	if start < _DIRECT {
		return _DIRECT, sp.direct[start:]
	}
	if start < _SINGLE {
		next, list := getNextDirectSectorListFromSingle(start-_DIRECT, sp.single)
		if list != nil {
			return next + _DIRECT, list
		}
		start = _SINGLE
	}
	if start < _DOUBLE {
		next, list := getNextDirectSectorListFromDouble(start-_SINGLE, sp.double)
		if list != nil {
			return next + _SINGLE, list
		}
		start = _DOUBLE
	}
	// start < _TRIPLE, always true since _TRIPLE is larger than uint32.
	next, list := getNextDirectSectorListFromTriple(start-_DOUBLE, sp.triple)
	if list != nil {
		return next + _DOUBLE, list
	}
	return 0, nil
}

func (sp *sectorPointer) GetLogicalSize() uint32 {
	return sp.logicalSectorLength
}

// Shrinks the logical size to the first allocated sector less than or
// equal to the supplied value. This is used when the sector mapper is
// truncated to find the first mapped sector in it's internal structure
// less than the supplied value.
//
// This function uses binary search with GetNextMappedSector function
// with a little bit of logic to turn the function monotonic when it
// reaches io.EOF.
//
// Finding the next mapped sector is roughly O(n^1/3) making this
// O(log(n)*n^(1/3)).
func (sp *sectorPointer) shrinkLogicalSize(n uint32) {
	// sort.Search does not support the entire uint32 range, so we
	// implement it ourselves.
	var i, j uint32 = 0, n
	for i < j {
		h := i + (j-i)/2
		next, err := sp.GetNextMappedSector(h)
		if err == io.EOF || next >= n {
			j = h
		} else {
			i = h + 1
		}
	}
	sp.logicalSectorLength = i
}

func clearSingle(x uint32, single *indirect[uint32]) bool {
	if single == nil {
		return true
	}
	for i := x; i < _SIZE; i++ {
		single.val[i] = 0
	}
	for i := uint32(0); i < x; i++ {
		if single.val[i] != 0 {
			return false
		}
	}
	return true
}

func clearDouble(x uint32, double *indirect[*indirect[uint32]]) bool {
	if double == nil {
		return true
	}
	start := x / _SIZE
	remainder := x % _SIZE
	for i := start; i < _SIZE; i++ {
		if remainder == 0 || clearSingle(remainder, double.val[i]) {
			double.val[i] = nil
		}
		remainder = 0
	}
	for i := uint32(0); i <= start; i++ {
		if double.val[i] != nil {
			return false
		}
	}
	return true
}

func clearTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) bool {
	if triple == nil {
		return true
	}
	start := x / (_SIZE * _SIZE)
	remainder := x % (_SIZE * _SIZE)
	for i := start; i < _SIZE; i++ {
		if remainder == 0 || clearDouble(remainder, triple.val[i]) {
			triple.val[i] = nil
		}
		remainder = 0
	}
	for i := uint32(0); i <= start; i++ {
		if triple.val[i] != nil {
			return false
		}
	}
	return true
}

func (sp *sectorPointer) Truncate(length uint32) {
	if length < sp.logicalSectorLength {
		sp.shrinkLogicalSize(length)
	}
	for i := length; i < _DIRECT; i++ {
		sp.direct[i] = 0
	}
	if length <= _DIRECT || clearSingle(length-_DIRECT, sp.single) {
		sp.single = nil
	}
	if length <= _SINGLE || clearDouble(length-_SINGLE, sp.double) {
		sp.double = nil
	}
	if length <= _DOUBLE || clearTriple(length-_DOUBLE, sp.triple) {
		sp.triple = nil
	}
}

func (sp *sectorPointer) setPhysicalIndex(logical, physical uint32) {
	sp.logicalSectorLength = max(sp.logicalSectorLength, logical+1)
	if logical < _DIRECT {
		sp.direct[logical] = physical
		return
	}
	if logical < _SINGLE {
		if sp.single == nil {
			sp.single = &indirect[uint32]{}
		}
		i := (logical - _DIRECT)
		sp.single.val[i] = physical
		return
	}
	if logical < _DOUBLE {
		i := (logical - _SINGLE) / _SIZE
		j := (logical - _SINGLE) - i*_SIZE
		if sp.double == nil {
			sp.double = &indirect[*indirect[uint32]]{}
		}
		if sp.double.val[i] == nil {
			sp.double.val[i] = &indirect[uint32]{}
		}
		sp.double.val[i].val[j] = physical
		return
	}
	// logical < _TRIPLE
	i := (logical - _DOUBLE) / _SIZE / _SIZE
	j := (logical-_DOUBLE)/_SIZE - i*_SIZE
	k := (logical - _DOUBLE) - i*_SIZE*_SIZE - j*_SIZE
	if sp.triple == nil {
		sp.triple = &indirect[*indirect[*indirect[uint32]]]{}
	}
	if sp.triple.val[i] == nil {
		sp.triple.val[i] = &indirect[*indirect[uint32]]{}
	}
	if sp.triple.val[i].val[j] == nil {
		sp.triple.val[i].val[j] = &indirect[uint32]{}
	}
	sp.triple.val[i].val[j].val[k] = physical
}

func (sp *sectorPointer) InsertSectorsContiguous(logical uint32, physical uint32, length uint32) error {
	for i := uint32(0); i < length; i++ {
		val := sp.GetPhysicalIndex(i + logical)
		if val != 0 {
			return status.Errorf(codes.Internal, "Attempted to insert a sector at logical adress %d but it is already occupied by %d", i+logical, val)
		}
		sp.setPhysicalIndex(i+logical, i+physical)
	}
	return nil
}
