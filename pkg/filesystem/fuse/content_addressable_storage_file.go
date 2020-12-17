// +build darwin linux

package fuse

import (
	"context"
	"io"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type contentAddressableStorageFile struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
	inodeNumber               uint64
	digest                    digest.Digest
	isExecutable              bool
}

// NewContentAddressableStorageFile creates a file that can be stored in
// an InMemoryDirectory that is directly backed by the Content
// Addressable Storage (CAS). The file is entirely immutable; it is only
// possible to read its contents.
func NewContentAddressableStorageFile(context context.Context, contentAddressableStorage blobstore.BlobAccess, errorLogger util.ErrorLogger, inodeNumber uint64, digest digest.Digest, isExecutable bool) NativeLeaf {
	return &contentAddressableStorageFile{
		context:                   context,
		contentAddressableStorage: contentAddressableStorage,
		errorLogger:               errorLogger,
		inodeNumber:               inodeNumber,
		digest:                    digest,
		isExecutable:              isExecutable,
	}
}

func (f *contentAddressableStorageFile) GetFileType() filesystem.FileType {
	if f.isExecutable {
		return filesystem.FileTypeExecutableFile
	}
	return filesystem.FileTypeRegularFile
}

func (f *contentAddressableStorageFile) Link() {
	// As this file is stateless, we don't need to do any explicit
	// bookkeeping for hardlinks.
}

func (f *contentAddressableStorageFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *contentAddressableStorageFile) Unlink() {
}

func (f *contentAddressableStorageFile) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	// This file is already backed by the Content Addressable
	// Storage. There is thus no need to upload it once again.
	//
	// The client that created this build action already called
	// FindMissingBlobs() on this file, so there's also a high
	// degree of certainty that this file won't disappear from the
	// Content Addressable Storage any time soon.
	return f.digest, nil
}

func (f *contentAddressableStorageFile) FUSEAccess(mask uint32) fuse.Status {
	var permitted uint32 = fuse.R_OK
	if f.isExecutable {
		permitted |= fuse.X_OK
	}
	if mask&^permitted != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *contentAddressableStorageFile) FUSEFallocate(off, size uint64) fuse.Status {
	return fuse.EBADF
}

func (f *contentAddressableStorageFile) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Size = uint64(f.digest.GetSizeBytes())
	out.Mode = fuse.S_IFREG | 0444
	if f.isExecutable {
		out.Mode |= 0111
	}
	out.Nlink = StatelessLeafLinkCount
}

func (f *contentAddressableStorageFile) FUSEGetDirEntry() fuse.DirEntry {
	return fuse.DirEntry{
		Mode: fuse.S_IFREG,
		Ino:  f.inodeNumber,
	}
}

func (f *contentAddressableStorageFile) FUSEGetXAttr(attr string, dest []byte) (uint32, fuse.Status) {
	// Provide an extended attribute that allows consumers to
	// reobtain the digest of the file without loading its actual
	// contents. This attribute is hidden from listxattr() to ensure
	// tools like cp(1) don't copy it to mutable files.
	//
	// More details: https://github.com/bazelbuild/bazel/pull/11662
	if attr == f.digest.GetHashXAttrName() {
		h := []byte(f.digest.GetHashBytes())
		if len(dest) < len(h) {
			return uint32(len(h)), fuse.ERANGE
		}
		copy(dest, h)
		return uint32(len(h)), fuse.OK
	}
	return 0, fuse.ENOATTR
}

func (f *contentAddressableStorageFile) FUSEOpen(flags uint32) fuse.Status {
	if flags&fuse.O_ANYWRITE != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *contentAddressableStorageFile) FUSERead(buf []byte, off uint64) (fuse.ReadResult, fuse.Status) {
	nRead, err := f.contentAddressableStorage.Get(f.context, f.digest).ReadAt(buf, int64(off))
	if err != nil && err != io.EOF {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to read from %s at offset %d", f.digest, off))
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf[:nRead]), fuse.OK
}

func (f *contentAddressableStorageFile) FUSEReadlink() ([]byte, fuse.Status) {
	return nil, fuse.EINVAL
}

func (f *contentAddressableStorageFile) FUSERelease() {}

func (f *contentAddressableStorageFile) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EACCES
	}
	f.FUSEGetAttr(out)
	return fuse.OK
}

func (f *contentAddressableStorageFile) FUSEWrite(buf []byte, off uint64) (uint32, fuse.Status) {
	panic("Request to write to read-only file should have been intercepted")
}
