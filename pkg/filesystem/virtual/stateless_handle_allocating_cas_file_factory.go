package virtual

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type statelessHandleAllocatingCASFileFactory struct {
	base      CASFileFactory
	allocator StatelessHandleAllocator
}

// NewStatelessHandleAllocatingCASFileFactory creates a decorator for
// CASFileFactory that creates read-only files for files stored in the
// Content Addressable Storage that have a stateless handle associated
// with them.
//
// This decorator is intended to be used in places where CASFileFactory
// is used to place files in mutable directories that properly track
// lifetimes of files. By making these files stateless, as opposed to
// resolvable, implementations of HandleAllocator may deduplicate
// multiple instances of the same file in the file system.
func NewStatelessHandleAllocatingCASFileFactory(base CASFileFactory, allocation StatelessHandleAllocation) CASFileFactory {
	cff := &statelessHandleAllocatingCASFileFactory{
		base: base,
	}
	cff.allocator = allocation.AsStatelessAllocator()
	return cff
}

func (cff *statelessHandleAllocatingCASFileFactory) LookupFile(blobDigest digest.Digest, isExecutable bool) NativeLeaf {
	return cff.allocator.
		New(&casFileID{
			blobDigest:   blobDigest,
			isExecutable: isExecutable,
		}).
		AsNativeLeaf(cff.base.LookupFile(blobDigest, isExecutable))
}

// casFileID is capable of converting the parameters that were used to
// construct a file through CASFileFactory to a unique identifier to be
// provided to StatelessHandleAllocator.
type casFileID struct {
	blobDigest   digest.Digest
	isExecutable bool
}

func (id *casFileID) WriteTo(w io.Writer) (nTotal int64, err error) {
	n, _ := ByteSliceID(id.blobDigest.GetKey(digest.KeyWithInstance)).WriteTo(w)
	nTotal += n
	if id.isExecutable {
		n, _ := w.Write([]byte{1})
		nTotal += int64(n)
	} else {
		n, _ := w.Write([]byte{0})
		nTotal += int64(n)
	}
	return
}
