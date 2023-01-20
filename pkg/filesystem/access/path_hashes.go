package access

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

const fnv1aPrime = 1099511628211

// PathHashes is a set of Bloom filter hashes corresponding to a given
// path in the input root.
type PathHashes struct {
	baseHash uint64
}

// NewPathHashesFromBaseHash creates a new set of Bloom filter hashes
// corresponding to an explicit base hash value.
func NewPathHashesFromBaseHash(baseHash uint64) PathHashes {
	return PathHashes{
		baseHash: baseHash,
	}
}

// GetBaseHash returns the base hash value of a set of Bloom filter
// hashes. This can be used to preserve and restore an instance of
// PathHashes.
func (hf PathHashes) GetBaseHash() uint64 {
	return hf.baseHash
}

// RootPathHashes is the set of Bloom filter hashes corresponding to the
// root directory of the input root (i.e., the empty path).
var RootPathHashes = PathHashes{
	// FNV-1a offset basis.
	baseHash: 14695981039346656037,
}

// AppendComponent returns a new set of Bloom filter hashes
// corresponding to a child of the current directory.
func (hf PathHashes) AppendComponent(name path.Component) PathHashes {
	// Path separator.
	baseHash := (hf.baseHash ^ '/') * fnv1aPrime

	// Filename.
	nameStr := name.String()
	for i := 0; i < len(nameStr); i++ {
		baseHash = (baseHash ^ uint64(nameStr[i])) * fnv1aPrime
	}
	return PathHashes{
		baseHash: baseHash,
	}
}

// Finalize the set of Bloom filter hashes corresponding to the current
// path, and return a PathHashIterator to extract these hashes.
func (hf PathHashes) Finalize() PathHashIterator {
	return PathHashIterator{
		nextHash: hf.baseHash,
	}
}

// PathHashIterator is capable of yielding a sequence of hashes
// corresponding to a given path.
type PathHashIterator struct {
	nextHash uint64
}

// GetNextHash progresses the PathHashIterator and returns the next hash
// corresponding to a given path.
func (hi *PathHashIterator) GetNextHash() uint64 {
	nextHash := hi.nextHash
	hi.nextHash = (nextHash ^ '/') * fnv1aPrime
	return nextHash
}
