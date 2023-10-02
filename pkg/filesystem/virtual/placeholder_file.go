package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// placeholderFile provides a common implementation for the part of the
// Leaf interface that is identical for all types of files that are
// merely placeholders on the file system, such as symbolic links,
// character devices and FIFOs.
type placeholderFile struct{}

func (placeholderFile) Link() Status {
	return StatusOK
}

func (placeholderFile) Unlink() {
}

func (placeholderFile) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a placeholder")
}

func (placeholderFile) GetContainingDigests() digest.Set {
	return digest.EmptySet
}

func (placeholderFile) VirtualAllocate(off, size uint64) Status {
	return StatusErrWrongType
}

func (placeholderFile) VirtualClose(shareAccess ShareMask) {}

func (placeholderFile) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	// Even though this file may not necessarily be a symbolic link,
	// the NFSv4 specification requires that NFS4ERR_SYMLINK is
	// returned for all irregular files.
	return StatusErrSymlink
}

func (placeholderFile) VirtualRead(buf []byte, offset uint64) (int, bool, Status) {
	panic("Request to read from special file should have been intercepted")
}

func (placeholderFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status) {
	panic("Request to seek on special file should have been intercepted")
}

func (placeholderFile) VirtualWrite(buf []byte, off uint64) (int, Status) {
	panic("Request to write to symbolic link should have been intercepted")
}
