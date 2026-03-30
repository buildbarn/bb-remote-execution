package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type handleAllocatingSymlinkFactory struct {
	base       SymlinkFactory
	allocator  StatelessHandleAllocator
	pathFormat path.Format
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
func NewHandleAllocatingSymlinkFactory(base SymlinkFactory, allocation StatelessHandleAllocation, pathFormat path.Format) SymlinkFactory {
	return &handleAllocatingSymlinkFactory{
		base:       base,
		allocator:  allocation.AsStatelessAllocator(),
		pathFormat: pathFormat,
	}
}

func (sf *handleAllocatingSymlinkFactory) LookupSymlink(target path.Parser) (LinkableLeaf, error) {
	builder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(target, scopeWalker); err != nil {
		return nil, err
	}
	targetStr, err := sf.pathFormat.GetString(builder)
	if err != nil {
		return nil, err
	}
	symlink, err := sf.base.LookupSymlink(target)
	if err != nil {
		return nil, err
	}
	return sf.allocator.
		New(ByteSliceID([]byte(targetStr))).
		AsLinkableLeaf(symlink), nil
}
