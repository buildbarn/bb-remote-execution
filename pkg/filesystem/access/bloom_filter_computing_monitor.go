package access

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// bloomFilterComputingState is the shared state that's referenced by
// all instances of BloomFilterComputingUnreadDirectoryMonitor and
// bloomFilterComputingReadDirectoryMonitor. It contains all of the
// hashes that should be encoded in the resulting Bloom filter.
type bloomFilterComputingState struct {
	directoriesResolved atomic.Uint64

	lock            sync.Mutex
	allHashes       map[PathHashes]struct{}
	directoriesRead uint64
	filesRead       uint64
}

// BloomFilterComputingUnreadDirectoryMonitor is an implementation of
// UnreadDirectoryMonitor that is capable of computing a Bloom filter of
// all of the paths of files and directories that have been read.
type BloomFilterComputingUnreadDirectoryMonitor struct {
	state  *bloomFilterComputingState
	hashes PathHashes
}

// NewBloomFilterComputingUnreadDirectoryMonitor creates an
// UnreadDirectoryMonitor that is capable of computing a Bloom filter of
// all of the paths of files and directories that have been read. The
// instance that is returned corresponds to the empty path (root
// directory).
func NewBloomFilterComputingUnreadDirectoryMonitor() *BloomFilterComputingUnreadDirectoryMonitor {
	udm := &BloomFilterComputingUnreadDirectoryMonitor{
		state: &bloomFilterComputingState{
			allHashes: map[PathHashes]struct{}{},
		},
		hashes: RootPathHashes,
	}
	// Assume the root directory is always resolved.
	udm.state.directoriesResolved.Store(1)
	return udm
}

var _ UnreadDirectoryMonitor = (*BloomFilterComputingUnreadDirectoryMonitor)(nil)

// ReadDirectory can be called to indicate that the contents of a
// directory have been read. It causes the directory to be added to the
// resulting Bloom filter.
func (udm *BloomFilterComputingUnreadDirectoryMonitor) ReadDirectory() ReadDirectoryMonitor {
	s := udm.state
	s.lock.Lock()
	s.directoriesRead++
	s.allHashes[udm.hashes] = struct{}{}
	s.lock.Unlock()

	return &bloomFilterComputingReadDirectoryMonitor{
		state:  s,
		hashes: udm.hashes,
	}
}

// largestPrimeOffsets is a table of the largest prime numbers below
// powers of 2. To save space, the delta against the powers of 2 are
// stored. Reference: https://oeis.org/A014234.
var largestPrimeOffsets = [...]uint8{
	1<<3 - 7,
	1<<4 - 13,
	1<<5 - 31,
	1<<6 - 61,
	1<<7 - 127,
	1<<8 - 251,
	1<<9 - 509,
	1<<10 - 1021,
	1<<11 - 2039,
	1<<12 - 4093,
	1<<13 - 8191,
	1<<14 - 16381,
	1<<15 - 32749,
	1<<16 - 65521,
	1<<17 - 131071,
	1<<18 - 262139,
	1<<19 - 524287,
	1<<20 - 1048573,
	1<<21 - 2097143,
	1<<22 - 4194301,
	1<<23 - 8388593,
	1<<24 - 16777213,
	1<<25 - 33554393,
	1<<26 - 67108859,
	1<<27 - 134217689,
	1<<28 - 268435399,
	1<<29 - 536870909,
	1<<30 - 1073741789,
	1<<31 - 2147483647,
	1<<32 - 4294967291,
	1<<33 - 8589934583,
	1<<34 - 17179869143,
	1<<35 - 34359738337,
	1<<36 - 68719476731,
	1<<37 - 137438953447,
	1<<38 - 274877906899,
	1<<39 - 549755813881,
	1<<40 - 1099511627689,
	1<<41 - 2199023255531,
	1<<42 - 4398046511093,
	1<<43 - 8796093022151,
	1<<44 - 17592186044399,
	1<<45 - 35184372088777,
	1<<46 - 70368744177643,
	1<<47 - 140737488355213,
	1<<48 - 281474976710597,
	1<<49 - 562949953421231,
	1<<50 - 1125899906842597,
	1<<51 - 2251799813685119,
	1<<52 - 4503599627370449,
	1<<53 - 9007199254740881,
	1<<54 - 18014398509481951,
	1<<55 - 36028797018963913,
	1<<56 - 72057594037927931,
	1<<57 - 144115188075855859,
	1<<58 - 288230376151711717,
	1<<59 - 576460752303423433,
	1<<60 - 1152921504606846883,
	1<<61 - 2305843009213693951,
	1<<62 - 4611686018427387847,
	1<<63 - 9223372036854775783,
}

// GetBloomFilter returns the Bloom filter that contains all of the
// paths of files and directories that have been read.
//
// The size of the resulting Bloom filter is based on the desired bits
// per element, and the maximum size in bits. In case it is limited by
// the maximum size, the resulting Bloom filter will be oversaturated,
// causing the probability of false positives to increase.
func (udm *BloomFilterComputingUnreadDirectoryMonitor) GetBloomFilter(bitsPerElement, maximumSizeBytes int) ([]byte, uint32) {
	s := udm.state
	s.lock.Lock()
	defer s.lock.Unlock()

	// Determine the size of the Bloom filter, taking both the
	// element count and size per element. The resulting size is the
	// largest prime below a power of 2, not exceeding the
	// configured maximum size.
	elementCount := len(s.allHashes)
	desiredSizeBits := elementCount * bitsPerElement
	sizeBits, sizeBytes := 7, 1
	for shift, largestPrimeOffset := range largestPrimeOffsets {
		newSizeBits := 8<<shift - int(largestPrimeOffset)
		newSizeBytes := newSizeBits/8 + 1
		if newSizeBytes > maximumSizeBytes {
			break
		}
		sizeBits, sizeBytes = newSizeBits, newSizeBytes
		if sizeBits >= desiredSizeBits {
			break
		}
	}

	// Determine the optimal number of hash functions to use.
	hashFunctions := uint32(1)
	if elementCount > 0 {
		hashFunctions = uint32(math.Round(float64(sizeBits) / float64(elementCount) * math.Ln2))
		if hashFunctions < 1 {
			hashFunctions = 1
		}
	}

	// Construct the Bloom filter using the desired size and number
	// of hash functions. The Bloom filter is terminated with a 1
	// bit, so that consumers can reobtain the exact size in bits.
	bloomFilter := make([]byte, sizeBytes)
	bloomFilter[sizeBytes-1] |= 1 << (sizeBits % 8)
	for hashes := range s.allHashes {
		hashIterator := hashes.Finalize()
		for i := uint32(0); i < hashFunctions; i++ {
			bit := hashIterator.GetNextHash() % uint64(sizeBits)
			bloomFilter[bit/8] |= 1 << (bit % 8)
		}
	}
	return bloomFilter, hashFunctions
}

// GetInputRootResourceUsage returns statistics on how many files and
// directories in an input root are being accessed. This message can be
// attached to the auxiliary metadata of ActionResult.
func (udm *BloomFilterComputingUnreadDirectoryMonitor) GetInputRootResourceUsage() *resourceusage.InputRootResourceUsage {
	s := udm.state
	s.lock.Lock()
	defer s.lock.Unlock()

	directoriesResolved := s.directoriesResolved.Load()
	return &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: directoriesResolved,
		DirectoriesRead:     s.directoriesRead,
		FilesRead:           s.filesRead,
	}
}

type bloomFilterComputingReadDirectoryMonitor struct {
	state  *bloomFilterComputingState
	hashes PathHashes
}

func (sdm *bloomFilterComputingReadDirectoryMonitor) ResolvedDirectory(name path.Component) UnreadDirectoryMonitor {
	s := sdm.state
	s.directoriesResolved.Add(1)

	return &BloomFilterComputingUnreadDirectoryMonitor{
		state:  s,
		hashes: sdm.hashes.AppendComponent(name),
	}
}

func (sdm *bloomFilterComputingReadDirectoryMonitor) ReadFile(name path.Component) {
	s := sdm.state
	s.lock.Lock()
	s.filesRead++
	s.allHashes[sdm.hashes.AppendComponent(name)] = struct{}{}
	s.lock.Unlock()
}
