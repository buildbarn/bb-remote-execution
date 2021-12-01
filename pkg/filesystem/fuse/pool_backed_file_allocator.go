//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"
	"io"
	"math"
	"sync"
	"syscall"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type poolBackedFileAllocator struct {
	pool                 re_filesystem.FilePool
	errorLogger          util.ErrorLogger
	inodeNumberGenerator random.ThreadSafeGenerator
}

// NewPoolBackedFileAllocator creates an allocator for a leaf node that
// may be stored in an PrepopulatedDirectory, representing a mutable
// regular file. All operations to mutate file contents (reads, writes
// and truncations) are forwarded to a file obtained from a FilePool.
//
// When the file becomes unreachable (i.e., both its link count and open
// file descriptor count reach zero), Close() is called on the
// underlying backing file descriptor. This may be used to request
// deletion from underlying storage.
func NewPoolBackedFileAllocator(pool re_filesystem.FilePool, errorLogger util.ErrorLogger, inodeNumberGenerator random.ThreadSafeGenerator) FileAllocator {
	return &poolBackedFileAllocator{
		pool:                 pool,
		errorLogger:          errorLogger,
		inodeNumberGenerator: inodeNumberGenerator,
	}
}

func (fa *poolBackedFileAllocator) NewFile(flags, mode uint32) (NativeLeaf, fuse.Status) {
	file, err := fa.pool.NewFile()
	if err != nil {
		fa.errorLogger.Log(util.StatusWrapf(err, "Failed to create new file"))
		return nil, fuse.EIO
	}
	return &fileBackedFile{
		inodeNumber: fa.inodeNumberGenerator.Uint64(),
		errorLogger: fa.errorLogger,

		file:            file,
		isExecutable:    (mode & 0o111) != 0,
		nlink:           1,
		openDescriptors: 1,
		unfreezeWakeup:  make(chan struct{}),
		cachedDigest:    digest.BadDigest,
	}, fuse.OK
}

type fileBackedFile struct {
	inodeNumber uint64
	errorLogger util.ErrorLogger

	lock                  sync.RWMutex
	file                  filesystem.FileReadWriter
	isExecutable          bool
	size                  uint64
	nlink                 uint32
	openDescriptors       uint
	openFrozenDescriptors uint
	unfreezeWakeup        chan struct{}
	cachedDigest          digest.Digest
}

// lockMutatingData picks up the exclusive lock of the file and waits
// for any pending uploads of the file to complete. This function needs
// to be called in operations that mutate f.file and f.size.
func (f *fileBackedFile) lockMutatingData() {
	f.lock.Lock()
	for f.openFrozenDescriptors > 0 {
		c := f.unfreezeWakeup
		f.lock.Unlock()
		<-c
		f.lock.Lock()
	}
}

func (f *fileBackedFile) acquire(frozen bool) bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.file == nil {
		return false
	}
	f.openDescriptors++
	if frozen {
		f.openFrozenDescriptors++
	}
	return true
}

func (f *fileBackedFile) release(frozen bool) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.openDescriptors == 0 {
		panic("Invalid open descriptor count")
	}
	f.openDescriptors--

	if frozen {
		if f.openFrozenDescriptors == 0 {
			panic("Invalid open frozen descriptor count")
		}
		f.openFrozenDescriptors--
		if f.openFrozenDescriptors == 0 {
			close(f.unfreezeWakeup)
			f.unfreezeWakeup = make(chan struct{})
		}
	}
	f.closeIfNeeded()
}

func (f *fileBackedFile) closeIfNeeded() {
	if f.nlink == 0 && f.openDescriptors == 0 {
		f.file.Close()
		f.file = nil
	}
}

func (f *fileBackedFile) GetFileType() filesystem.FileType {
	f.lock.RLock()
	defer f.lock.RUnlock()

	if f.isExecutable {
		return filesystem.FileTypeExecutableFile
	}
	return filesystem.FileTypeRegularFile
}

func (f *fileBackedFile) Link() {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.file == nil {
		panic("Attempted to link removed file")
	}
	f.nlink++
}

func (f *fileBackedFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *fileBackedFile) Unlink() {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.nlink == 0 {
		panic("Invalid link count")
	}
	f.nlink--
	f.closeIfNeeded()
}

func (f *fileBackedFile) getCachedDigest() digest.Digest {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cachedDigest
}

// updateCachedDigest returns the digest of the file. It either returns
// a cached value, or computes the digest and caches it. It is only safe
// to call this function while the file is frozen (i.e., calling
// f.acquire(true)).
func (f *fileBackedFile) updateCachedDigest(digestFunction digest.Function) (digest.Digest, error) {
	// Check whether the cached digest we have is still valid.
	if cachedDigest := f.getCachedDigest(); cachedDigest != digest.BadDigest && cachedDigest.UsesDigestFunction(digestFunction) {
		return cachedDigest, nil
	}

	// If not, compute a new digest.
	digestGenerator := digestFunction.NewGenerator()
	if _, err := io.Copy(digestGenerator, io.NewSectionReader(f, 0, math.MaxInt64)); err != nil {
		return digest.BadDigest, util.StatusWrapWithCode(err, codes.Internal, "Failed to compute file digest")
	}
	newDigest := digestGenerator.Sum()

	// Store the resulting cached digest.
	f.lock.Lock()
	f.cachedDigest = newDigest
	f.lock.Unlock()
	return newDigest, nil
}

func (f *fileBackedFile) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	// Create a file handle that temporarily freezes the contents of
	// this file. This ensures that the file's contents don't change
	// between the digest computation and upload phase. This allows
	// us to safely use NewValidatedBufferFromFileReader().
	if !f.acquire(true) {
		return digest.BadDigest, status.Error(codes.NotFound, "File was unlinked before uploading could start")
	}

	blobDigest, err := f.updateCachedDigest(digestFunction)
	if err != nil {
		f.Close()
		return digest.BadDigest, err
	}

	if err := contentAddressableStorage.Put(
		ctx,
		blobDigest,
		buffer.NewValidatedBufferFromReaderAt(f, blobDigest.GetSizeBytes())); err != nil {
		return digest.BadDigest, util.StatusWrap(err, "Failed to upload file")
	}
	return blobDigest, nil
}

func (f *fileBackedFile) GetContainingDigests() digest.Set {
	return digest.EmptySet
}

func (f *fileBackedFile) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	fileStatus := &remoteoutputservice.FileStatus_File{}
	if digestFunction != nil {
		if !f.acquire(true) {
			return nil, status.Error(codes.NotFound, "File was unlinked before digest computation could start")
		}
		blobDigest, err := f.updateCachedDigest(*digestFunction)
		f.release(true)
		if err != nil {
			return nil, err
		}
		fileStatus.Digest = blobDigest.GetProto()
	}
	return &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: fileStatus,
		},
	}, nil
}

func (f *fileBackedFile) AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component) {
	// Because bb_clientd is mostly intended to be used in
	// combination with remote execution, we don't want to spend too
	// much effort persisting locally created output files. Those
	// may easily exceed the size of the state file, making
	// finalization of builds expensive.
	//
	// Most of the time people still enable remote caching for
	// locally running actions, or have Build Event Streams enabled.
	// In that case there is a fair chance that the file is present
	// in the CAS anyway.
	//
	// In case we have a cached digest for the file available, let's
	// generate an entry for it in the persistent state file. This
	// means that after a restart, the file is silently converted to
	// a CAS-backed file. If it turns out this assumption is
	// incorrect, StartBuild() will clean up the file for us.
	if cachedDigest := f.getCachedDigest(); cachedDigest != digest.BadDigest {
		directory.Files = append(directory.Files, &remoteexecution.FileNode{
			Name:         name.String(),
			Digest:       f.cachedDigest.GetProto(),
			IsExecutable: f.isExecutable,
		})
	}
}

func (f *fileBackedFile) Close() error {
	f.release(true)
	return nil
}

func (f *fileBackedFile) ReadAt(b []byte, off int64) (int, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.file.ReadAt(b, off)
}

func (f *fileBackedFile) FUSEAccess(mask uint32) fuse.Status {
	var permitted uint32 = fuse.R_OK | fuse.W_OK
	f.lock.RLock()
	if f.isExecutable {
		permitted |= fuse.X_OK
	}
	f.lock.RUnlock()
	if mask&^permitted != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *fileBackedFile) FUSEFallocate(off, size uint64) fuse.Status {
	f.lockMutatingData()
	defer f.lock.Unlock()

	if end := uint64(off) + uint64(size); f.size < end {
		if err := f.file.Truncate(int64(end)); err != nil {
			f.errorLogger.Log(util.StatusWrapf(err, "Failed to truncate file to length %d", end))
			return fuse.EIO
		}
		f.cachedDigest = digest.BadDigest
		f.size = end
	}
	return fuse.OK
}

func (f *fileBackedFile) fuseGetAttrLocked(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Size = f.size
	out.Mode = fuse.S_IFREG | 0o666
	if f.isExecutable {
		out.Mode |= 0o111
	}
	out.Nlink = f.nlink
}

func (f *fileBackedFile) FUSEGetAttr(out *fuse.Attr) {
	f.lock.RLock()
	f.fuseGetAttrLocked(out)
	f.lock.RUnlock()
}

func (f *fileBackedFile) FUSEGetDirEntry() fuse.DirEntry {
	return fuse.DirEntry{
		Mode: fuse.S_IFREG,
		Ino:  f.inodeNumber,
	}
}

func (f *fileBackedFile) FUSELseek(in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	var regionType filesystem.RegionType
	switch in.Whence {
	case unix.SEEK_DATA:
		regionType = filesystem.Data
	case unix.SEEK_HOLE:
		regionType = filesystem.Hole
	default:
		panic("Requests for other seek modes should have been intercepted")
	}

	f.lock.Lock()
	off, err := f.file.GetNextRegionOffset(int64(in.Offset), regionType)
	f.lock.Unlock()
	if err == io.EOF {
		return fuse.Status(syscall.ENXIO)
	} else if err != nil {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to get next region offset at offset %d", in.Offset))
		return fuse.EIO
	}
	out.Offset = uint64(off)
	return fuse.OK
}

func (f *fileBackedFile) FUSEOpen(flags uint32) fuse.Status {
	if !f.acquire(false) {
		// The file was removed through the
		// PrepopulatedDirectory API, but is still being
		// accessed through the FUSE file system. Treat the file
		// as stale if this were to happen.
		return fuse.Status(syscall.ESTALE)
	}
	return fuse.OK
}

func (f *fileBackedFile) FUSERead(buf []byte, off uint64) (fuse.ReadResult, fuse.Status) {
	f.lock.Lock()
	defer f.lock.Unlock()

	nRead, err := f.file.ReadAt(buf, int64(off))
	if err != nil && err != io.EOF {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to read from file at offset %d", off))
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf[:nRead]), fuse.OK
}

func (f *fileBackedFile) FUSEReadlink() ([]byte, fuse.Status) {
	return nil, fuse.EINVAL
}

func (f *fileBackedFile) FUSERelease() {
	f.release(false)
}

func (f *fileBackedFile) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}

	if in.Valid&fuse.FATTR_SIZE != 0 {
		f.lockMutatingData()
	} else {
		f.lock.Lock()
	}
	defer f.lock.Unlock()

	if in.Valid&fuse.FATTR_SIZE != 0 {
		if err := f.file.Truncate(int64(in.Size)); err != nil {
			f.errorLogger.Log(util.StatusWrapf(err, "Failed to truncate file to length %d", in.Size))
			return fuse.EIO
		}
		f.cachedDigest = digest.BadDigest
		f.size = in.Size
	}
	if in.Valid&fuse.FATTR_MODE != 0 {
		f.isExecutable = (in.Mode & 0o111) != 0
	}
	f.fuseGetAttrLocked(out)
	return fuse.OK
}

func (f *fileBackedFile) FUSEWrite(buf []byte, offset uint64) (uint32, fuse.Status) {
	f.lockMutatingData()
	defer f.lock.Unlock()

	nWritten, err := f.file.WriteAt(buf, int64(offset))
	if nWritten > 0 {
		f.cachedDigest = digest.BadDigest
		if end := offset + uint64(nWritten); f.size < end {
			f.size = end
		}
	}
	if err != nil {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to write to file at offset %d", offset))
		return uint32(nWritten), fuse.EIO
	}
	return uint32(nWritten), fuse.OK
}
