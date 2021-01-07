// +build darwin linux

package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type symlink struct {
	inodeNumber uint64
	target      string
}

// NewSymlink creates a symbolic link node that may be added to an
// InMemoryDirectory. It is an inmutable file; the target to which it
// points can only be altered by replacing the node entirely (e.g., by
// first unlinking it from the directory).
func NewSymlink(target string) NativeLeaf {
	// Simply let the inode number be based on the target. That way
	// identical symlinks are deduplicated by the kernel.
	inodeNumber := sha256.Sum256([]byte(target))
	return &symlink{
		inodeNumber: binary.LittleEndian.Uint64(inodeNumber[:]),
		target:      target,
	}
}

func (f *symlink) GetFileType() filesystem.FileType {
	return filesystem.FileTypeSymlink
}

func (f *symlink) Link() {
}

func (f *symlink) Readlink() (string, error) {
	return f.target, nil
}

func (f *symlink) Unlink() {
}

func (f *symlink) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a symbolic link")
}

func (f *symlink) FUSEAccess(mask uint32) fuse.Status {
	return fuse.OK
}

func (f *symlink) FUSEFallocate(off, size uint64) fuse.Status {
	return fuse.ENOSYS
}

func (f *symlink) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Size = uint64(len(f.target))
	out.Mode = fuse.S_IFLNK | 0777
	out.Nlink = StatelessLeafLinkCount
}

func (f *symlink) FUSEGetDirEntry() fuse.DirEntry {
	return fuse.DirEntry{
		Mode: fuse.S_IFLNK,
		Ino:  f.inodeNumber,
	}
}

func (f *symlink) FUSEOpen(flags uint32) fuse.Status {
	panic("Request to open symbolic link should have been intercepted")
}

func (f *symlink) FUSERead(buf []byte, off uint64) (fuse.ReadResult, fuse.Status) {
	panic("Request to read from symbolic link should have been intercepted")
}

func (f *symlink) FUSEReadlink() ([]byte, fuse.Status) {
	return []byte(f.target), fuse.OK
}

func (f *symlink) FUSERelease() {}

func (f *symlink) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EINVAL
	}
	f.FUSEGetAttr(out)
	return fuse.OK
}

func (f *symlink) FUSEWrite(buf []byte, off uint64) (uint32, fuse.Status) {
	panic("Request to write to symbolic link should have been intercepted")
}
