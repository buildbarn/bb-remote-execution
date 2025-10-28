package virtual

import "github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"

type handleAllocatingFileAllocator struct {
	base      FileAllocator
	allocator StatefulHandleAllocator
}

// NewHandleAllocatingFileAllocator creates a decorator for
// FileAllocator that creates mutable files that have a stateful handle
// associated with them. This gives ever mutable file its own inode
// number and link count.
func NewHandleAllocatingFileAllocator(base FileAllocator, allocator StatefulHandleAllocator) FileAllocator {
	return &handleAllocatingFileAllocator{
		base:      base,
		allocator: allocator,
	}
}

func (fa *handleAllocatingFileAllocator) NewFile(holeSource pool.HoleSource, isExecutable bool, size uint64, shareAccess ShareMask) (LinkableLeaf, Status) {
	leaf, s := fa.base.NewFile(holeSource, isExecutable, size, shareAccess)
	if s != StatusOK {
		return nil, s
	}
	return fa.allocator.New().AsLinkableLeaf(leaf), StatusOK
}
