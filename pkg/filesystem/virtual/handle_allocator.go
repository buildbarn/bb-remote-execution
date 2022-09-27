package virtual

import (
	"encoding/binary"
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// StatefulHandleAllocator is responsible for allocating new file
// handles, giving files and directories stored in the file system their
// own identity and lifetime. The exact meaning of identity is
// implementation specific. In the case of FUSE it can be an inode
// number/node ID, while in the case of NFSv4 it corresponds to the
// value of a 128-byte nfs_fh4.
type StatefulHandleAllocator interface {
	New() StatefulHandleAllocation
}

// StatefulHandleAllocation corresponds to an allocation of a file
// handle that is unique, meaning it can be used to identify stateful,
// mutable files or directories.
//
// Exactly one call to one of the methods on this interface needs to be
// performed to convert the allocation to an actual use of the file
// handle.
type StatefulHandleAllocation interface {
	StatelessHandleAllocation

	AsStatefulDirectory(directory Directory) StatefulDirectoryHandle
}

// StatefulDirectoryHandle is a handle that needs to be embedded into
// stateful directories. It can be used to report mutations to the
// directory through NotifyRemoval(), or report deletion through
// Release().
//
// The directory type that embeds the handle must call GetAttributes()
// as part of Directory.VirtualGetAttributes() to augment the attributes
// with an inode number and/or file handle.
type StatefulDirectoryHandle interface {
	GetAttributes(requested AttributesMask, attributes *Attributes)
	NotifyRemoval(name path.Component)
	Release()
}

// StatelessHandleAllocator is responsible for allocating file handles
// of files that are stateless, immutable files.
//
// For every handle that is allocated, an identifier needs to be
// provided in the form of an io.WriterTo. This may be used to give
// files with the same identifier the same underlying file handle or
// inode number.
//
// NOTE: Care must be taken that the provided identifier is properly
// terminated or is prefixed with its own length. Stateless handle
// allocators may be nested, causing their identifiers to be
// concatenated. This could cause ambiguity if this rule is not
// followed.
type StatelessHandleAllocator interface {
	New(id io.WriterTo) StatelessHandleAllocation
}

// StatelessHandleAllocation corresponds to an allocation of a file
// handle that is stateless, meaning it can be used to identify
// stateless files or directories.
type StatelessHandleAllocation interface {
	ResolvableHandleAllocation

	AsStatelessAllocator() StatelessHandleAllocator
}

// ResolvableHandleAllocator is responsible for allocating file handles
// that are not only stateless, but can also be trivially reconstructed
// based on a small, bounded amount of information.
//
// This kind of allocator is generally used by bb_clientd's cas/
// subdirectory. This directory allows for the exploration of arbitrary
// objects stored in the Content Addressable Storage (CAS). This
// allocator can be used to store the digest of such objects in the file
// handle, meaning bb_clientd doesn't need to track state for each of
// the files individually.
//
// The exact amount of space that can be stored in a file handle is
// protocol specific. NFSv3 and NFSv4 use file handles that are 64 and
// 128 bytes in size, respectively.
type ResolvableHandleAllocator interface {
	New(id io.WriterTo) ResolvableHandleAllocation
}

// HandleResolver is a method that is used by ResolvableHandleAllocator
// to reconstruct files based on the identifiers provided to New().
//
// TODO: Implementations of this method must currently make sure that
// directories and leaves that are returned are decorated with a handle
// allocation. Can't we let implementations of ResolvableHandleAllocator
// do this? That way resolvers may remain simple.
type HandleResolver func(r io.ByteReader) (DirectoryChild, Status)

// ResolvableHandleAllocation corresponds to an allocation of a file
// handle that is not only stateless, but can also be trivially
// reconstructed based on a small, bounded of information.
//
// TODO: ResolvableHandleAllocators can be nested. The resolver provided
// to AsResolvableAllocator() is only used for the first level. We could
// eliminate the argument, but that makes composition harder.
type ResolvableHandleAllocation interface {
	AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator
	AsStatelessDirectory(directory Directory) Directory
	AsNativeLeaf(leaf NativeLeaf) NativeLeaf
	AsLeaf(leaf Leaf) Leaf
}

// ByteSliceID is a helper type for consumers of
// StatelessHandleAllocator and ResolvableHandleAllocator. It uses the
// contents of a byte slice as an object ID. The byte slice will be
// prefixed with its own length, so that no ambiguity exists if
// allocators are nested.
type ByteSliceID []byte

// WriteTo writes the length of a byte slice and its contents to the Writer.
func (data ByteSliceID) WriteTo(w io.Writer) (nTotal int64, err error) {
	var targetSize [binary.MaxVarintLen64]byte
	n, err := w.Write(targetSize[:binary.PutUvarint(targetSize[:], uint64(len(data)))])
	nTotal += int64(n)
	n, err = w.Write(data)
	nTotal += int64(n)
	return
}
