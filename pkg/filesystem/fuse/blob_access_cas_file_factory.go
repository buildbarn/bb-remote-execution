// +build darwin linux

package fuse

import (
	"context"
	"io"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type blobAccessCASFileFactory struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
	executableInodeNumberTree InodeNumberTree
	fileInodeNumberTree       InodeNumberTree
}

// NewBlobAccessCASFileFactory creates a CASFileFactory that can be used
// to create FUSE files that are directly backed by BlobAccess. Files
// created by this factory are entirely immutable; it is only possible
// to read their contents.
func NewBlobAccessCASFileFactory(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, errorLogger util.ErrorLogger) CASFileFactory {
	return &blobAccessCASFileFactory{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		errorLogger:               errorLogger,
		executableInodeNumberTree: NewRandomInodeNumberTree(),
		fileInodeNumberTree:       NewRandomInodeNumberTree(),
	}
}

func (cff *blobAccessCASFileFactory) GetFileInodeNumber(blobDigest digest.Digest, isExecutable bool) uint64 {
	// Generate an inode number based on the digest of the
	// underlying object. That way two identical files share the
	// same inode number, meaning that the kernel only caches its
	// contents once.
	//
	// Do make sure that two identical files with different +x bits
	// have a different inode number to prevent them from being
	// merged. Because every build action uses its own
	// CASFileFactory, inode numbers are not shared across build
	// actions. Each build action has its own timeout. We wouldn't
	// want build actions to receive timeouts prematurely.
	inodeNumberTree := &cff.fileInodeNumberTree
	if isExecutable {
		inodeNumberTree = &cff.executableInodeNumberTree
	}
	return inodeNumberTree.AddString(blobDigest.GetKey(digest.KeyWithInstance)).Get()
}

func (cff *blobAccessCASFileFactory) LookupFile(digest digest.Digest, isExecutable bool, out *fuse.Attr) NativeLeaf {
	baseFile := blobAccessCASFile{
		factory:     cff,
		inodeNumber: cff.GetFileInodeNumber(digest, isExecutable),
		digest:      digest,
	}
	var f NativeLeaf
	if isExecutable {
		f = &executableBlobAccessCASFile{
			blobAccessCASFile: baseFile,
		}
	} else {
		f = &regularBlobAccessCASFile{
			blobAccessCASFile: baseFile,
		}
	}
	f.FUSEGetAttr(out)
	return f
}

// blobAccessCASFile is the base type for all BlobAccess backed CAS
// files. This type is intentionally kept as small as possible, as many
// instances may be created. All shared options are shared in the
// factory object.
type blobAccessCASFile struct {
	factory     *blobAccessCASFileFactory
	inodeNumber uint64
	digest      digest.Digest
}

func (f *blobAccessCASFile) Link() {
	// As this file is stateless, we don't need to do any explicit
	// bookkeeping for hardlinks.
}

func (f *blobAccessCASFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *blobAccessCASFile) Unlink() {
}

func (f *blobAccessCASFile) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	// This file is already backed by the Content Addressable
	// Storage. There is thus no need to upload it once again.
	//
	// The client that created this build action already called
	// FindMissingBlobs() on this file, so there's also a high
	// degree of certainty that this file won't disappear from the
	// Content Addressable Storage any time soon.
	return f.digest, nil
}

func (f *blobAccessCASFile) GetContainingDigests() digest.Set {
	return f.digest.ToSingletonSet()
}

func (f *blobAccessCASFile) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	fileStatusFile := remoteoutputservice.FileStatus_File{}
	if digestFunction != nil {
		// Assume that the file uses the same hash algorithm as
		// the provided digest function. Incompatible files are
		// removed from storage at the start of the build.
		fileStatusFile.Digest = f.digest.GetProto()
	}
	return &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &fileStatusFile,
		},
	}, nil
}

func (f *blobAccessCASFile) FUSEFallocate(off, size uint64) fuse.Status {
	return fuse.EBADF
}

func (f *blobAccessCASFile) fuseGetAttrCommon(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Size = uint64(f.digest.GetSizeBytes())
	out.Nlink = StatelessLeafLinkCount
}

func (f *blobAccessCASFile) FUSEGetDirEntry() fuse.DirEntry {
	return fuse.DirEntry{
		Mode: fuse.S_IFREG,
		Ino:  f.inodeNumber,
	}
}

func (f *blobAccessCASFile) FUSEOpen(flags uint32) fuse.Status {
	if flags&fuse.O_ANYWRITE != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *blobAccessCASFile) FUSERead(buf []byte, off uint64) (fuse.ReadResult, fuse.Status) {
	nRead, err := f.factory.contentAddressableStorage.Get(f.factory.context, f.digest).ReadAt(buf, int64(off))
	if err != nil && err != io.EOF {
		f.factory.errorLogger.Log(util.StatusWrapf(err, "Failed to read from %s at offset %d", f.digest, off))
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf[:nRead]), fuse.OK
}

func (f *blobAccessCASFile) FUSEReadlink() ([]byte, fuse.Status) {
	return nil, fuse.EINVAL
}

func (f *blobAccessCASFile) FUSERelease() {}

func (f *blobAccessCASFile) fuseSetAttrCommon(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *blobAccessCASFile) FUSEWrite(buf []byte, off uint64) (uint32, fuse.Status) {
	panic("Request to write to read-only file should have been intercepted")
}

// regularBlobAccessCASFile is the type BlobAccess backed files that are
// not executable (-x).
type regularBlobAccessCASFile struct {
	blobAccessCASFile
}

func (f *regularBlobAccessCASFile) GetFileType() filesystem.FileType {
	return filesystem.FileTypeRegularFile
}

func (f *regularBlobAccessCASFile) FUSEAccess(mask uint32) fuse.Status {
	if mask&^fuse.R_OK != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *regularBlobAccessCASFile) FUSEGetAttr(out *fuse.Attr) {
	f.fuseGetAttrCommon(out)
	out.Mode = fuse.S_IFREG | 0o444
}

func (f *regularBlobAccessCASFile) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if s := f.fuseSetAttrCommon(in, out); s != fuse.OK {
		return s
	}
	f.FUSEGetAttr(out)
	return fuse.OK
}

// regularBlobAccessCASFile is the type BlobAccess backed files that are
// executable (+x).
type executableBlobAccessCASFile struct {
	blobAccessCASFile
}

func (f *executableBlobAccessCASFile) GetFileType() filesystem.FileType {
	return filesystem.FileTypeExecutableFile
}

func (f *executableBlobAccessCASFile) FUSEAccess(mask uint32) fuse.Status {
	if mask&^(fuse.R_OK|fuse.X_OK) != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *executableBlobAccessCASFile) FUSEGetAttr(out *fuse.Attr) {
	f.fuseGetAttrCommon(out)
	out.Mode = fuse.S_IFREG | 0o555
}

func (f *executableBlobAccessCASFile) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if s := f.fuseSetAttrCommon(in, out); s != fuse.OK {
		return s
	}
	f.FUSEGetAttr(out)
	return fuse.OK
}
