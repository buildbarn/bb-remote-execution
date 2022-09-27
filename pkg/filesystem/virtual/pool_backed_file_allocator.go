package virtual

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
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type poolBackedFileAllocator struct {
	pool        re_filesystem.FilePool
	errorLogger util.ErrorLogger
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
func NewPoolBackedFileAllocator(pool re_filesystem.FilePool, errorLogger util.ErrorLogger) FileAllocator {
	return &poolBackedFileAllocator{
		pool:        pool,
		errorLogger: errorLogger,
	}
}

func (fa *poolBackedFileAllocator) NewFile(isExecutable bool, size uint64) (NativeLeaf, Status) {
	file, err := fa.pool.NewFile()
	if err != nil {
		fa.errorLogger.Log(util.StatusWrapf(err, "Failed to create new file"))
		return nil, StatusErrIO
	}
	if size > 0 {
		if err := file.Truncate(int64(size)); err != nil {
			fa.errorLogger.Log(util.StatusWrapf(err, "Failed to truncate file to length %d", size))
			file.Close()
			return nil, StatusErrIO
		}
	}
	return &fileBackedFile{
		errorLogger: fa.errorLogger,

		file:         file,
		isExecutable: isExecutable,
		size:         size,
		// The initial reference count of newly created files is
		// always two, as they will be linked into the file
		// system and opened for writing.
		referenceCount: 2,
		unfreezeWakeup: make(chan struct{}),
		cachedDigest:   digest.BadDigest,
	}, StatusOK
}

type fileBackedFile struct {
	errorLogger util.ErrorLogger

	lock                  sync.RWMutex
	file                  filesystem.FileReadWriter
	isExecutable          bool
	size                  uint64
	referenceCount        uint
	openFrozenDescriptors uint
	unfreezeWakeup        chan struct{}
	cachedDigest          digest.Digest
	changeID              uint64
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

func (f *fileBackedFile) acquireFrozen() bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.referenceCount == 0 {
		return false
	}
	f.referenceCount++
	f.openFrozenDescriptors++
	return true
}

func (f *fileBackedFile) release(count uint, frozen bool) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.referenceCount < count {
		panic("Invalid reference count")
	}
	f.referenceCount -= count

	if frozen {
		if f.openFrozenDescriptors < count {
			panic("Invalid open frozen descriptor count")
		}
		f.openFrozenDescriptors -= count
		if f.openFrozenDescriptors == 0 {
			close(f.unfreezeWakeup)
			f.unfreezeWakeup = make(chan struct{})
		}
	}
	f.closeIfNeeded()
}

func (f *fileBackedFile) closeIfNeeded() {
	if f.referenceCount == 0 {
		f.file.Close()
		f.file = nil
	}
}

func (f *fileBackedFile) Link() Status {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.referenceCount == 0 {
		return StatusErrStale
	}
	f.referenceCount++
	return StatusOK
}

func (f *fileBackedFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *fileBackedFile) Unlink() {
	f.release(1, false)
}

func (f *fileBackedFile) getCachedDigest() digest.Digest {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cachedDigest
}

// updateCachedDigest returns the digest of the file. It either returns
// a cached value, or computes the digest and caches it. It is only safe
// to call this function while the file is frozen (i.e., calling
// f.acquireFrozen()).
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
	if !f.acquireFrozen() {
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
		if !f.acquireFrozen() {
			return nil, status.Error(codes.NotFound, "File was unlinked before digest computation could start")
		}
		blobDigest, err := f.updateCachedDigest(*digestFunction)
		f.release(1, true)
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
	f.release(1, true)
	return nil
}

func (f *fileBackedFile) ReadAt(b []byte, off int64) (int, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.file.ReadAt(b, off)
}

func (f *fileBackedFile) VirtualAllocate(off, size uint64) Status {
	f.lockMutatingData()
	defer f.lock.Unlock()

	if end := uint64(off) + uint64(size); f.size < end {
		if s := f.virtualTruncate(end); s != StatusOK {
			return s
		}
	}
	return StatusOK
}

// virtualGetAttributesUnlocked gets file attributes that can be
// obtained without picking up any locks.
func (f *fileBackedFile) virtualGetAttributesUnlocked(attributes *Attributes) {
	attributes.SetFileType(filesystem.FileTypeRegularFile)
}

// virtualGetAttributesUnlocked gets file attributes that can only be
// obtained while picking up the file's lock.
func (f *fileBackedFile) virtualGetAttributesLocked(attributes *Attributes) {
	attributes.SetChangeID(f.changeID)
	permissions := PermissionsRead | PermissionsWrite
	if f.isExecutable {
		permissions |= PermissionsExecute
	}
	attributes.SetPermissions(permissions)
	attributes.SetSizeBytes(f.size)
}

func (f *fileBackedFile) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	// Only pick up the file's lock when the caller requests
	// attributes that require locking.
	f.virtualGetAttributesUnlocked(attributes)
	if requested&(AttributesMaskChangeID|AttributesMaskPermissions|AttributesMaskSizeBytes) != 0 {
		f.lock.RLock()
		f.virtualGetAttributesLocked(attributes)
		f.lock.RUnlock()
	}
}

func (f *fileBackedFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status) {
	f.lock.Lock()
	if offset >= f.size {
		f.lock.Unlock()
		return nil, StatusErrNXIO
	}
	off, err := f.file.GetNextRegionOffset(int64(offset), regionType)
	f.lock.Unlock()
	if err == io.EOF {
		// NFSv4's SEEK operation with NFS4_CONTENT_DATA differs
		// from lseek(). If there is a hole at the end of the
		// file, we should return success with sr_eof set,
		// instead of failing with ENXIO.
		return nil, StatusOK
	} else if err != nil {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to get next region offset at offset %d", offset))
		return nil, StatusErrIO
	}
	result := uint64(off)
	return &result, StatusOK
}

func (f *fileBackedFile) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.referenceCount == 0 {
		return StatusErrStale
	}

	// Handling of O_TRUNC.
	if options.Truncate {
		if s := f.virtualTruncate(0); s != StatusOK {
			return s
		}
	}

	f.referenceCount++
	f.virtualGetAttributesUnlocked(attributes)
	f.virtualGetAttributesLocked(attributes)
	return StatusOK
}

func (f *fileBackedFile) VirtualRead(buf []byte, off uint64) (int, bool, Status) {
	f.lock.Lock()
	defer f.lock.Unlock()

	buf, eof := BoundReadToFileSize(buf, off, f.size)
	if len(buf) > 0 {
		if n, err := f.file.ReadAt(buf, int64(off)); n != len(buf) {
			f.errorLogger.Log(util.StatusWrapf(err, "Failed to read from file at offset %d", off))
			return 0, false, StatusErrIO
		}
	}
	return len(buf), eof, StatusOK
}

func (f *fileBackedFile) VirtualReadlink(ctx context.Context) ([]byte, Status) {
	return nil, StatusErrInval
}

func (f *fileBackedFile) VirtualClose(count uint) {
	f.release(count, false)
}

func (f *fileBackedFile) virtualTruncate(size uint64) Status {
	if err := f.file.Truncate(int64(size)); err != nil {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to truncate file to length %d", size))
		return StatusErrIO
	}
	f.cachedDigest = digest.BadDigest
	f.size = size
	f.changeID++
	return StatusOK
}

func (f *fileBackedFile) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	sizeBytes, hasSizeBytes := in.GetSizeBytes()
	if hasSizeBytes {
		f.lockMutatingData()
	} else {
		f.lock.Lock()
	}
	defer f.lock.Unlock()

	if hasSizeBytes {
		if s := f.virtualTruncate(sizeBytes); s != StatusOK {
			return s
		}
	}
	if permissions, ok := in.GetPermissions(); ok {
		f.isExecutable = (permissions & PermissionsExecute) != 0
		f.changeID++
	}

	f.virtualGetAttributesUnlocked(out)
	f.virtualGetAttributesLocked(out)
	return StatusOK
}

func (f *fileBackedFile) VirtualWrite(buf []byte, offset uint64) (int, Status) {
	f.lockMutatingData()
	defer f.lock.Unlock()

	nWritten, err := f.file.WriteAt(buf, int64(offset))
	if nWritten > 0 {
		f.cachedDigest = digest.BadDigest
		if end := offset + uint64(nWritten); f.size < end {
			f.size = end
		}
		f.changeID++
	}
	if err != nil {
		f.errorLogger.Log(util.StatusWrapf(err, "Failed to write to file at offset %d", offset))
		return nWritten, StatusErrIO
	}
	return nWritten, StatusOK
}
