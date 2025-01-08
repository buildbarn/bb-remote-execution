package virtual

import (
	"context"
	"io"
	"math"
	"sync"
	"syscall"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	poolBackedFileAllocatorPrometheusMetrics sync.Once

	poolBackedFileAllocatorWritableFileUploadDelaySeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "virtual",
			Name:      "pool_backed_file_allocator_writable_file_upload_delay_seconds",
			Help:      "The amount of time uploading a pool-backed file to the Content Addressable Storage was delayed, waiting for writable file descriptors to be closed.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		})
	poolBackedFileAllocatorWritableFileUploadDelayTimeouts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "virtual",
			Name:      "pool_backed_file_allocator_writable_file_upload_delay_timeouts_total",
			Help:      "Total number times the contents of a pool-backed file were uploaded into the Content Addressable Storage while one or more writable file descriptors were present, due to the maximum permitted delay being reached.",
		})
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
	poolBackedFileAllocatorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(poolBackedFileAllocatorWritableFileUploadDelaySeconds)
		prometheus.MustRegister(poolBackedFileAllocatorWritableFileUploadDelayTimeouts)
	})

	return &poolBackedFileAllocator{
		pool:        pool,
		errorLogger: errorLogger,
	}
}

func (fa *poolBackedFileAllocator) NewFile(isExecutable bool, size uint64, shareAccess ShareMask) (LinkableLeaf, Status) {
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
	f := &fileBackedFile{
		errorLogger: fa.errorLogger,

		file:           file,
		isExecutable:   isExecutable,
		size:           size,
		referenceCount: 1,
		cachedDigest:   digest.BadDigest,
	}
	f.acquireShareAccessLocked(shareAccess)
	return f, StatusOK
}

type fileBackedFile struct {
	errorLogger util.ErrorLogger

	lock                     sync.RWMutex
	file                     filesystem.FileReadWriter
	isExecutable             bool
	size                     uint64
	referenceCount           uint
	writableDescriptorsCount uint
	noMoreWritersWakeup      chan struct{}
	frozenDescriptorsCount   uint
	unfreezeWakeup           chan struct{}
	cachedDigest             digest.Digest
	changeID                 uint64
}

// lockMutatingData picks up the exclusive lock of the file and waits
// for any pending uploads of the file to complete. This function needs
// to be called in operations that mutate f.file and f.size.
func (f *fileBackedFile) lockMutatingData() {
	f.lock.Lock()
	for f.frozenDescriptorsCount > 0 {
		if f.unfreezeWakeup == nil {
			f.unfreezeWakeup = make(chan struct{})
		}
		c := f.unfreezeWakeup
		f.lock.Unlock()
		<-c
		f.lock.Lock()
	}
}

func (f *fileBackedFile) openReadFrozen() (filesystem.FileReader, bool) {
	if f.referenceCount == 0 {
		return nil, false
	}
	f.referenceCount++
	f.frozenDescriptorsCount++
	return &frozenFileBackedFile{
		file: f,
	}, true
}

func (f *fileBackedFile) waitAndOpenReadFrozen(writableFileDelay <-chan struct{}) (filesystem.FileReader, bool) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.writableDescriptorsCount > 0 {
		// Process table cleaning should have cleaned up any
		// file descriptors belonging to files in the input
		// root. Yet we are still seeing the file being opened
		// for writing. This is bad, as it means that data may
		// still be present in the kernel's page cache.
		start := time.Now()
		deadlineExceeded := false
		for f.writableDescriptorsCount > 0 && !deadlineExceeded {
			if f.noMoreWritersWakeup == nil {
				f.noMoreWritersWakeup = make(chan struct{})
			}
			c := f.noMoreWritersWakeup

			f.lock.Unlock()
			select {
			case <-writableFileDelay:
				deadlineExceeded = true
			case <-c:
			}
			f.lock.Lock()
		}

		if f.writableDescriptorsCount > 0 {
			poolBackedFileAllocatorWritableFileUploadDelayTimeouts.Inc()
		} else {
			poolBackedFileAllocatorWritableFileUploadDelaySeconds.Observe(time.Now().Sub(start).Seconds())
		}
	}

	return f.openReadFrozen()
}

func (f *fileBackedFile) acquireShareAccessLocked(shareAccess ShareMask) {
	f.referenceCount += shareAccess.Count()
	if shareAccess&ShareMaskWrite != 0 {
		f.writableDescriptorsCount++
	}
}

func (f *fileBackedFile) releaseReferencesLocked(count uint) {
	if f.referenceCount < count {
		panic("Invalid reference count")
	}
	f.referenceCount -= count
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

func (f *fileBackedFile) Unlink() {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.releaseReferencesLocked(1)
}

func (f *fileBackedFile) getCachedDigest() digest.Digest {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cachedDigest
}

// updateCachedDigest returns the digest of the file. It either returns
// a cached value, or computes the digest and caches it. It is only safe
// to call this function while the file is frozen (i.e., calling
// f.openReadFrozen()).
func (f *fileBackedFile) updateCachedDigest(digestFunction digest.Function, frozenFile filesystem.FileReader) (digest.Digest, error) {
	// Check whether the cached digest we have is still valid.
	if cachedDigest := f.getCachedDigest(); cachedDigest != digest.BadDigest && cachedDigest.UsesDigestFunction(digestFunction) {
		return cachedDigest, nil
	}

	// If not, compute a new digest.
	digestGenerator := digestFunction.NewGenerator(math.MaxInt64)
	if _, err := io.Copy(digestGenerator, io.NewSectionReader(frozenFile, 0, math.MaxInt64)); err != nil {
		return digest.BadDigest, util.StatusWrapWithCode(err, codes.Internal, "Failed to compute file digest")
	}
	newDigest := digestGenerator.Sum()

	// Store the resulting cached digest.
	f.lock.Lock()
	f.cachedDigest = newDigest
	f.lock.Unlock()
	return newDigest, nil
}

func (f *fileBackedFile) uploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
	frozenFile, success := f.waitAndOpenReadFrozen(writableFileUploadDelay)
	if !success {
		return digest.BadDigest, status.Error(codes.NotFound, "File was unlinked before uploading could start")
	}

	blobDigest, err := f.updateCachedDigest(digestFunction, frozenFile)
	if err != nil {
		frozenFile.Close()
		return digest.BadDigest, err
	}

	if err := contentAddressableStorage.Put(
		ctx,
		blobDigest,
		buffer.NewValidatedBufferFromReaderAt(frozenFile, blobDigest.GetSizeBytes())); err != nil {
		return digest.BadDigest, util.StatusWrap(err, "Failed to upload file")
	}
	return blobDigest, nil
}

func (f *fileBackedFile) getBazelOutputServiceStat(digestFunction *digest.Function) (*bazeloutputservice.BatchStatResponse_Stat, error) {
	var locator *anypb.Any
	f.lock.Lock()
	if f.writableDescriptorsCount == 0 {
		frozenFile, success := f.openReadFrozen()
		f.lock.Unlock()
		if !success {
			return nil, status.Error(codes.NotFound, "File was unlinked before digest computation could start")
		}
		defer frozenFile.Close()

		blobDigest, err := f.updateCachedDigest(*digestFunction, frozenFile)
		if err != nil {
			return nil, err
		}
		locator, err = anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
			Digest: blobDigest.GetProto(),
		})
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to marshal locator")
		}
	} else {
		// Don't report the digest if the file is opened for
		// writing. The kernel may still hold on to data that
		// needs to be written, meaning that digests computed on
		// this end are inaccurate.
		//
		// By not reporting the digest, the client will
		// recompute it itself. This will be consistent with
		// what's stored in the kernel's page cache.
		f.lock.Unlock()
	}
	return &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, nil
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

func (f *fileBackedFile) VirtualApply(data any) bool {
	switch p := data.(type) {
	case *ApplyReadlink:
		p.Err = syscall.EINVAL
	case *ApplyUploadFile:
		p.Digest, p.Err = f.uploadFile(p.Context, p.ContentAddressableStorage, p.DigestFunction, p.WritableFileUploadDelay)
	case *ApplyGetBazelOutputServiceStat:
		p.Stat, p.Err = f.getBazelOutputServiceStat(p.DigestFunction)
	case *ApplyAppendOutputPathPersistencyDirectoryNode:
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
			p.Directory.Files = append(p.Directory.Files, &remoteexecution.FileNode{
				Name:         p.Name.String(),
				Digest:       f.cachedDigest.GetProto(),
				IsExecutable: f.isExecutable,
			})
		}
	case *ApplyOpenReadFrozen:
		if frozenFile, success := f.waitAndOpenReadFrozen(p.WritableFileDelay); success {
			p.Reader = frozenFile
		} else {
			p.Err = status.Error(codes.NotFound, "File was unlinked before file could be opened")
		}
	default:
		return false
	}
	return true
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
	if options.Truncate {
		f.lockMutatingData()
	} else {
		f.lock.Lock()
	}
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

	f.acquireShareAccessLocked(shareAccess)
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

func (f *fileBackedFile) VirtualClose(shareAccess ShareMask) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if shareAccess&ShareMaskWrite != 0 {
		if f.writableDescriptorsCount == 0 {
			panic("Invalid writable descriptor count")
		}
		f.writableDescriptorsCount--
		if f.writableDescriptorsCount == 0 && f.noMoreWritersWakeup != nil {
			close(f.noMoreWritersWakeup)
			f.noMoreWritersWakeup = nil
		}
	}
	f.releaseReferencesLocked(shareAccess.Count())
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

// frozenFileBackedFile is a handle to a file whose contents have been
// frozen and has been opened for reading. The file will be unfrozen
// when closed.
type frozenFileBackedFile struct {
	file *fileBackedFile
}

func (ff *frozenFileBackedFile) Close() error {
	f := ff.file
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.frozenDescriptorsCount == 0 {
		panic("Invalid open frozen descriptor count")
	}
	f.frozenDescriptorsCount--
	if f.frozenDescriptorsCount == 0 && f.unfreezeWakeup != nil {
		close(f.unfreezeWakeup)
		f.unfreezeWakeup = nil
	}

	f.releaseReferencesLocked(1)
	ff.file = nil
	return nil
}

func (ff *frozenFileBackedFile) GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error) {
	f := ff.file
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.file.GetNextRegionOffset(offset, regionType)
}

func (ff *frozenFileBackedFile) ReadAt(b []byte, off int64) (int, error) {
	f := ff.file
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.file.ReadAt(b, off)
}
