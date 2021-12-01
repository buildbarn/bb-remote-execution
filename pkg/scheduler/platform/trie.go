package platform

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Trie is a prefix tree for instance names and platform properties. It
// can be used by implementations of BuildQueue to match instance names
// and platform properties provides as part of execution requests
// against individual platform queues.
//
// For every key stored in the trie, an integer value is tracked. This
// can, for example, be used by the caller to look up a corresponding
// value in a contiguous list.
type Trie struct {
	platforms map[string]*digest.InstanceNameTrie
}

// NewTrie creates a new Trie that is initialized with no elements.
func NewTrie() *Trie {
	return &Trie{
		platforms: map[string]*digest.InstanceNameTrie{},
	}
}

// ContainsExact returns whether the trie contains a key that is exactly
// the same as the one provided.
func (t *Trie) ContainsExact(key Key) bool {
	if pt, ok := t.platforms[key.platform]; ok {
		return pt.ContainsExact(key.instanceNamePrefix)
	}
	return false
}

// GetExact returns the value associated with the key. If none of the
// keys provided to Set() are exactly the same as the key provided to
// GetExact(), this function returns -1.
func (t *Trie) GetExact(key Key) int {
	if pt, ok := t.platforms[key.platform]; ok {
		return pt.GetExact(key.instanceNamePrefix)
	}
	return -1
}

// GetLongestPrefix returns the value associated with the longest
// matching instance name prefix, having the same platform properties.
// If none of the keys provided to Set() have an instance name that is a
// prefix, or have the same platform properties as that of the key
// provided to GetLongestPrefix(), this function returns -1.
func (t *Trie) GetLongestPrefix(key Key) int {
	if pt, ok := t.platforms[key.platform]; ok {
		return pt.GetLongestPrefix(key.instanceNamePrefix)
	}
	return -1
}

// Remove a value associated with a key.
func (t *Trie) Remove(key Key) {
	if t.platforms[key.platform].Remove(key.instanceNamePrefix) {
		// The trie is now empty. Remove it from the map.
		delete(t.platforms, key.platform)
	}
}

// Set a key in the trie to a given integer value.
func (t *Trie) Set(key Key, value int) {
	pt, ok := t.platforms[key.platform]
	if !ok {
		pt = digest.NewInstanceNameTrie()
		t.platforms[key.platform] = pt
	}
	pt.Set(key.instanceNamePrefix, value)
}
