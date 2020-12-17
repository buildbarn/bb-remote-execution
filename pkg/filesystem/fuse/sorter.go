package fuse

import (
	"math/rand"
	"sort"
)

// Sorter is a function type for a sorting algorithm. Its signature is
// identical to the sort.Sort() function.
//
// This type is used by SimpleRawFileSystem to make the policy for
// sorting the results of FUSEReadDir() and FUSEReadDirPlus()
// configurable. Depending on the use case, it is desirable to use a
// deterministic algorithm (e.g., alphabetic sorting) or an
// undeterministic one (e.g., random shuffling).
type Sorter func(data sort.Interface)

var _ Sorter = sort.Sort

// Shuffle elements in a list using the Fisher-Yates algorithm.
func Shuffle(data sort.Interface) {
	rand.Shuffle(data.Len(), data.Swap)
}
