//go:build freebsd || linux
// +build freebsd linux

package fuse

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

type negativeEntryCachingRawFileSystem struct {
	RawFileSystem
}

// NewNegativeEntryCachingRawFileSystem creates a decorator for
// RawFileSystem that answers Lookup() calls for names that do not exist
// with a negative cache entry, instead of ENOENT.
//
// A negative cache entry is a successful reply that has node ID zero.
// The kernel remembers that the name does not exist for as long as the
// entry validity stored in the EntryOut, instead of calling into the
// file system again for every probe. This is beneficial for workloads
// that probe many names that do not exist, such as module resolution in
// Node.js.
//
// The entry validity is not set by this decorator. It should be
// combined with NewDefaultAttributesInjectingRawFileSystem, placed
// around this decorator, to fill in a non-zero entry validity. Without
// one, the kernel discards negative cache entries immediately.
//
// This decorator is opt in, as the kernel will not notice names that
// are created behind its back until the entry validity expires. It is
// only safe to use if all mutations of the directory hierarchy are
// performed through the mount, or if the entry validity is chosen to be
// short enough for staleness to be acceptable.
func NewNegativeEntryCachingRawFileSystem(base RawFileSystem) RawFileSystem {
	return &negativeEntryCachingRawFileSystem{
		RawFileSystem: base,
	}
}

func (rfs *negativeEntryCachingRawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	s := rfs.RawFileSystem.Lookup(cancel, header, name, out)
	if s != fuse.ENOENT {
		return s
	}
	out.NodeId = 0
	return fuse.OK
}
