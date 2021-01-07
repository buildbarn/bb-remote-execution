// +build darwin linux

package fuse

import (
	"context"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type device struct {
	inodeNumber  uint64
	fileType     uint32
	deviceNumber uint32
}

// NewDevice creates a node that may be used as a character device,
// block device, FIFO or UNIX domain socket. Nodes of these types are
// mere placeholders. The kernel is responsible for capturing calls to
// open() and connect().
func NewDevice(inodeNumber uint64, mode, deviceNumber uint32) NativeLeaf {
	return &device{
		inodeNumber:  inodeNumber,
		fileType:     mode & syscall.S_IFMT,
		deviceNumber: deviceNumber,
	}
}

func (f *device) GetFileType() filesystem.FileType {
	return filesystem.FileTypeOther
}

func (f *device) Link() {
}

func (f *device) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *device) Unlink() {
}

func (f *device) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a device")
}

func (f *device) FUSEAccess(mask uint32) fuse.Status {
	if mask&^(fuse.R_OK|fuse.W_OK) != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *device) FUSEFallocate(off, size uint64) fuse.Status {
	return fuse.ENOSYS
}

func (f *device) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Mode = f.fileType | 0666
	out.Nlink = StatelessLeafLinkCount
	out.Rdev = f.deviceNumber
}

func (f *device) FUSEGetDirEntry() fuse.DirEntry {
	return fuse.DirEntry{
		Mode: f.fileType,
		Ino:  f.inodeNumber,
	}
}

func (f *device) FUSEOpen(flags uint32) fuse.Status {
	panic("Request to open device should have been intercepted")
}

func (f *device) FUSERead(buf []byte, off uint64) (fuse.ReadResult, fuse.Status) {
	panic("Request to read from device should have been intercepted")
}

func (f *device) FUSEReadlink() ([]byte, fuse.Status) {
	return nil, fuse.EINVAL
}

func (f *device) FUSERelease() {}

func (f *device) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EINVAL
	}
	f.FUSEGetAttr(out)
	return fuse.OK
}

func (f *device) FUSEWrite(buf []byte, off uint64) (uint32, fuse.Status) {
	panic("Request to write to device should have been intercepted")
}
