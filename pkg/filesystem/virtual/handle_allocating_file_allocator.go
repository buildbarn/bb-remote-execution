package virtual

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

func (fa *handleAllocatingFileAllocator) NewFile(isExecutable bool, size uint64, shareAccess ShareMask) (NativeLeaf, Status) {
	leaf, s := fa.base.NewFile(isExecutable, size, shareAccess)
	if s != StatusOK {
		return nil, s
	}
	return fa.allocator.New().AsNativeLeaf(leaf), StatusOK
}
