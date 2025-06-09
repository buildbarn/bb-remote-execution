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
	_SIZE   = 1024
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

func (sp *sectorPointer) GetNextUnmappedSector(logical uint32) (uint32, error) {
	panic("Not implemented")
}

func (sp *sectorPointer) GetPhysicalIndex(logical uint32) uint32 {
	if logical < _DIRECT {
		return sp.direct[logical]
	}
	if logical < _SINGLE {
		i := logical - _DIRECT
		if sp.single == nil {
			return 0
		}
		return sp.single.val[i]
	}
	if logical < _DOUBLE {
		offset := logical - _SINGLE
		i := offset / _SIZE
		j := offset - i*_SIZE
		if sp.double == nil {
			return 0
		}
		if sp.double.val[i] == nil {
			return 0
		}
		return sp.double.val[i].val[j]
	}
	if logical < _TRIPLE {
		offset := logical - _DOUBLE
		i := offset / _SIZE / _SIZE
		j := (offset - i*_SIZE*_SIZE) / _SIZE
		k := offset - i*_SIZE*_SIZE - j*_SIZE
		if sp.triple == nil {
			return 0
		}
		if sp.triple.val[i] == nil {
			return 0
		}
		if sp.triple.val[i].val[j] == nil {
			return 0
		}
		return sp.triple.val[i].val[j].val[k]
	}
	return 0
}

func getNextDirectSectorListFromSingle(x uint32, single *indirect[uint32]) (uint32, []uint32) {
	if x >= _SIZE {
		return 0, nil
	}
	return _SIZE, single.val[x:]
}

func getNextDirectSectorListFromDouble(x uint32, double *indirect[*indirect[uint32]]) (uint32, []uint32) {
	if x >= _SIZE*_SIZE {
		return 0, nil
	}
	start := x / _SIZE
	remainder := x % _SIZE
	for i := start; i < _SIZE; i++ {
		if double.val[i] != nil {
			next, list := getNextDirectSectorListFromSingle(remainder, double.val[i])
			if list != nil {
				return next + i*_SIZE, list
			}
		}
		remainder = 0
	}
	return 0, nil
}

func getNextDirectSectorListFromTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) (uint32, []uint32) {
	if x >= _SIZE*_SIZE*_SIZE {
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
		if sp.single != nil {
			next, list := getNextDirectSectorListFromSingle(start-_DIRECT, sp.single)
			if list != nil {
				return next + _DIRECT, list
			}
		}
		start = _SINGLE
	}
	if start < _DOUBLE {
		if sp.double != nil {
			next, list := getNextDirectSectorListFromDouble(start-_SINGLE, sp.double)
			if list != nil {
				return next + _SINGLE, list
			}
		}
		start = _DOUBLE
	}
	if start < _TRIPLE {
		if sp.triple != nil {
			next, list := getNextDirectSectorListFromTriple(start-_DOUBLE, sp.triple)
			if list != nil {
				return next + _DOUBLE, list
			}
		}
		start = _TRIPLE
	}
	return 0, nil
}

func (sp *sectorPointer) GetLogicalSize() uint32 {
	if sp.logicalSectorLength == math.MaxUint32 {
		sp.updateLogicalSize()
	}
	return sp.logicalSectorLength
}

func (sp *sectorPointer) updateLogicalSize() {
	// TODO: This can potentially be optimized by using
	// GetNextMappedSector and GetNextUnmappedSector one at a time.
	size, err := sp.GetNextMappedSector(0)
	if err == io.EOF {
		sp.logicalSectorLength = 0
		return
	}
	for {
		next, err := sp.GetNextMappedSector(size + 1)
		if err == nil {
			size = next
		} else if err == io.EOF {
			break
		}
	}
	sp.logicalSectorLength = size + 1
}

func clearSingle(x uint32, single *indirect[uint32]) {
	for i := x; i < _SIZE; i++ {
		single.val[i] = 0
	}
}

func clearDouble(x uint32, double *indirect[*indirect[uint32]]) {
	start := x / _SIZE
	remainder := x % _SIZE
	for i := start; i < _SIZE; i++ {
		if double.val[i] != nil {
			if remainder == 0 {
				double.val[i] = nil
			} else {
				clearSingle(remainder, double.val[i])
			}
		}
		remainder = 0
	}
}

func clearTriple(x uint32, triple *indirect[*indirect[*indirect[uint32]]]) {
	start := x / (_SIZE * _SIZE)
	remainder := x % (_SIZE * _SIZE)
	for i := start; i < _SIZE; i++ {
		if triple.val[i] != nil {
			if remainder == 0 {
				triple.val[i] = nil
			} else {
				clearDouble(remainder, triple.val[i])
			}
		}
		remainder = 0
	}
}

func (sp *sectorPointer) Truncate(length uint32) {
	if length < sp.logicalSectorLength {
		sp.logicalSectorLength = math.MaxUint32
	}
	for i := length; i < _DIRECT; i++ {
		sp.direct[i] = 0
	}
	if length <= _DIRECT {
		sp.single = nil
	}
	if sp.single != nil {
		clearSingle(length-_DIRECT, sp.single)
	}
	if length <= _SINGLE {
		sp.double = nil
	}
	if sp.double != nil {
		clearDouble(length-_SINGLE, sp.double)
	}
	if length <= _DOUBLE {
		sp.triple = nil
	}
	if sp.triple != nil {
		clearTriple(length-_DOUBLE, sp.triple)
	}
}

func (sp *sectorPointer) setPhysicalIndex(logical, physical uint32) error {
	if logical >= _TRIPLE {
		return status.Errorf(codes.InvalidArgument, "Attempted to set logical sector %d, which is beyond the maximum representable by this data structure (%d)", logical, _TRIPLE-1)
	}
	sp.logicalSectorLength = max(sp.logicalSectorLength, logical+1)
	if logical < _DIRECT {
		sp.direct[logical] = physical
		return nil
	}
	if logical < _SINGLE {
		if sp.single == nil {
			sp.single = &indirect[uint32]{}
		}
		i := (logical - _DIRECT)
		sp.single.val[i] = physical
		return nil
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
		return nil
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
	return nil
}

func (sp *sectorPointer) InsertSectorsContiguous(logical uint32, physical uint32, length uint32) error {
	for i := uint32(0); i < length; i++ {
		val := sp.GetPhysicalIndex(i + logical)
		if val != 0 {
			return status.Errorf(codes.Internal, "Attempted to insert a sector at logical adress %d but it is already occupied by %d", i+logical, val)
		}
		err := sp.setPhysicalIndex(i+logical, i+physical)
		if err != nil {
			return status.Errorf(codes.Internal, "Unexpected error when inserting a sector at logical adress %d: %v", i+logical, err)
		}
	}
	return nil
}
