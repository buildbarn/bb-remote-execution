package virtual

import (
	"bytes"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type resolvableHandleAllocatingCASFileFactory struct {
	base      CASFileFactory
	allocator *ResolvableDigestHandleAllocator
}

// NewResolvableHandleAllocatingCASFileFactory creates a decorator for
// CASFileFactory that creates read-only files for files stored in the
// Content Addressable Storage that have a stateless handle associated
// with them.
//
// This decorator is intended to be used in places where CASFileFactory
// is used to hand out files with an indefinite lifetime, such as
// bb_clientd's "cas" directory. File handles will be larger, as the
// hash, size and executable bit of of the file will be stored in the
// file handle.
func NewResolvableHandleAllocatingCASFileFactory(base CASFileFactory, allocation StatelessHandleAllocation) CASFileFactory {
	cff := &resolvableHandleAllocatingCASFileFactory{
		base: base,
	}
	cff.allocator = NewResolvableDigestHandleAllocator(allocation, cff.resolve)
	return cff
}

func (cff *resolvableHandleAllocatingCASFileFactory) LookupFile(blobDigest digest.Digest, isExecutable bool) NativeLeaf {
	var isExecutableField [1]byte
	if isExecutable {
		isExecutableField[0] = 1
	}
	return cff.allocator.
		New(blobDigest).
		AsResolvableAllocator(func(r io.ByteReader) (Directory, Leaf, Status) {
			return cff.resolve(blobDigest, r)
		}).
		New(bytes.NewBuffer(isExecutableField[:])).
		AsNativeLeaf(cff.base.LookupFile(blobDigest, isExecutable))
}

func (cff *resolvableHandleAllocatingCASFileFactory) resolve(blobDigest digest.Digest, remainder io.ByteReader) (Directory, Leaf, Status) {
	isExecutable, err := remainder.ReadByte()
	if err != nil {
		return nil, nil, StatusErrBadHandle
	}
	switch isExecutable {
	case 0:
		return nil, cff.LookupFile(blobDigest, false), StatusOK
	case 1:
		return nil, cff.LookupFile(blobDigest, true), StatusOK
	default:
		return nil, nil, StatusErrBadHandle
	}
}
