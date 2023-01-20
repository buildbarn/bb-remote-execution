package access

import (
	"math/bits"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BloomFilterReader is a helper type for doing lookups against Bloom filters
// that were generated using BloomFilterComputingUnreadDirectoryMonitor.
type BloomFilterReader struct {
	bloomFilter   []byte
	sizeBits      int
	hashFunctions uint32
}

// NewBloomFilterReader checks the validity of a Bloom filter, and
// returns a reader type when valid.
func NewBloomFilterReader(bloomFilter []byte, hashFunctions uint32) (*BloomFilterReader, error) {
	// Derive the exact size in bits. The last byte contains padding
	// of the form "100..." that we need to strip. For that we need
	// to count the leading zeros of the last byte.
	if len(bloomFilter) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Bloom filter is empty")
	}
	leadingZeros := bits.LeadingZeros8(uint8(bloomFilter[len(bloomFilter)-1]))
	if leadingZeros == 8 {
		return nil, status.Error(codes.InvalidArgument, "Bloom filter's trailing byte is not properly padded")
	}

	// Put an upper limit on the number of hash functions to use, as
	// we don't want malformed Bloom filters to lead to deadlocks.
	// Even if the number of hash functions is intentionally set
	// above this limit, checking fewer is correct. It will merely
	// leads to an increase in false positives.
	if maximumHashFunctions := uint32(1000); hashFunctions > maximumHashFunctions {
		hashFunctions = maximumHashFunctions
	}

	return &BloomFilterReader{
		bloomFilter:   bloomFilter,
		sizeBits:      len(bloomFilter)*8 - leadingZeros - 1,
		hashFunctions: hashFunctions,
	}, nil
}

// Contains returns whether a path is contained in the Bloom filter.
func (r *BloomFilterReader) Contains(pathHashes PathHashes) bool {
	hashIterator := pathHashes.Finalize()
	for i := uint32(0); i < r.hashFunctions; i++ {
		bit := hashIterator.GetNextHash() % uint64(r.sizeBits)
		if r.bloomFilter[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}
