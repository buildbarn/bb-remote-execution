package virtual

import (
	"bytes"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// DigestHandleResolver is a handle resolver that needs to be
// implemented by users of ResolvableDigestHandleAllocator. This
// callback is responsible for looking up a file or directory that
// corresponds to a given REv2 digest that was reobtained from a file
// handle.
type DigestHandleResolver func(blobDigest digest.Digest, remainder io.ByteReader) (Directory, Leaf, Status)

// ResolvableDigestHandleAllocator is a convenience type for the handle
// allocator API, used for identifying objects by REv2 digest. It is
// used by bb_clientd's "cas" directory to give unique file handles to
// objects stored in the Content Addressable Storage.
//
// Because REv2 objects that include their instance name are too long to
// fit in NFS file handles, this type uses a stateless allocator for the
// instance name part. It uses a resolvable allocator only for the
// object hash and size, as those are generally small enough to fit in
// the file handle.
//
// This means that every time a new instance name is used, a resolver
// gets leaked. This is acceptable for most use cases, as the instance
// names in use tend to be limited.
type ResolvableDigestHandleAllocator struct {
	allocator StatelessHandleAllocator
	resolver  DigestHandleResolver
}

// NewResolvableDigestHandleAllocator creates a new
// NewResolvableDigestHandleAllocator.
func NewResolvableDigestHandleAllocator(allocation StatelessHandleAllocation, resolver DigestHandleResolver) *ResolvableDigestHandleAllocator {
	return &ResolvableDigestHandleAllocator{
		allocator: allocation.AsStatelessAllocator(),
		resolver:  resolver,
	}
}

// New creates a new handle allocation for an object with a given digest.
func (a *ResolvableDigestHandleAllocator) New(blobDigest digest.Digest) ResolvableHandleAllocation {
	instanceName := blobDigest.GetInstanceName()
	resolver := a.resolver
	return a.allocator.
		New(ByteSliceID([]byte(instanceName.String()))).
		AsResolvableAllocator(func(r io.ByteReader) (Directory, Leaf, Status) {
			blobDigest, err := instanceName.NewDigestFromCompactBinary(r)
			if err != nil {
				return nil, nil, StatusErrBadHandle
			}
			return resolver(blobDigest, r)
		}).
		New(bytes.NewBuffer(blobDigest.GetCompactBinary()))
}
