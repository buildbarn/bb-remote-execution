package virtual

type handleAllocatingSymlinkFactory struct {
	base      SymlinkFactory
	allocator StatelessHandleAllocator
}

// NewHandleAllocatingSymlinkFactory is a decorator for SymlinkFactory
// that creates symbolic link nodes that have a stateless handle
// associated with them.
//
// Because symbolic link contents can be long, it is not possible to use
// a resolvable allocator here, as that would cause the full contents of
// the symbolic link to become part of the file handle, which is
// undesirable. In the case of NFS we want these nodes to be explicitly
// tracked, using an invisible link count.
func NewHandleAllocatingSymlinkFactory(base SymlinkFactory, allocation StatelessHandleAllocation) SymlinkFactory {
	return &handleAllocatingSymlinkFactory{
		base:      base,
		allocator: allocation.AsStatelessAllocator(),
	}
}

func (sf *handleAllocatingSymlinkFactory) LookupSymlink(target []byte) NativeLeaf {
	return sf.allocator.
		New(ByteSliceID(target)).
		AsNativeLeaf(sf.base.LookupSymlink(target))
}
