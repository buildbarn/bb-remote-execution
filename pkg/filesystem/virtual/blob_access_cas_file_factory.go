package virtual

import (
	"context"
	"syscall"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type blobAccessCASFileFactory struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
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
	}
}

func (cff *blobAccessCASFileFactory) LookupFile(blobDigest digest.Digest, isExecutable bool, readMonitor FileReadMonitor) LinkableLeaf {
	if readMonitor != nil {
		panic("The read monitor should have been set up by StatelessHandleAllocatingCASFileFactory")
	}
	baseFile := blobAccessCASFile{
		factory: cff,
		digest:  blobDigest,
	}
	if isExecutable {
		return &executableBlobAccessCASFile{blobAccessCASFile: baseFile}
	}
	return &regularBlobAccessCASFile{blobAccessCASFile: baseFile}
}

// blobAccessCASFile is the base type for all BlobAccess backed CAS
// files. This type is intentionally kept as small as possible, as many
// instances may be created. All shared options are shared in the
// factory object.
type blobAccessCASFile struct {
	factory *blobAccessCASFileFactory
	digest  digest.Digest
}

func (f *blobAccessCASFile) Link() Status {
	// As this file is stateless, we don't need to do any explicit
	// bookkeeping for hardlinks.
	return StatusOK
}

func (f *blobAccessCASFile) Unlink() {
}

func (f *blobAccessCASFile) VirtualAllocate(off, size uint64) Status {
	return StatusErrWrongType
}

func (f *blobAccessCASFile) virtualApplyCommon(data any) bool {
	switch p := data.(type) {
	case *ApplyReadlink:
		p.Err = syscall.EINVAL
	case *ApplyUploadFile:
		// This file is already backed by the Content Addressable
		// Storage. There is thus no need to upload it once again.
		//
		// The client that created this build action already called
		// FindMissingBlobs() on this file, so there's also a high
		// degree of certainty that this file won't disappear from the
		// Content Addressable Storage any time soon.
		p.Digest = f.digest
	case *ApplyGetContainingDigests:
		p.ContainingDigests = f.digest.ToSingletonSet()
	case *ApplyGetBazelOutputServiceStat:
		// Assume that the file uses the same hash algorithm as
		// the provided digest function. Incompatible files are
		// removed from storage at the start of the build.
		if locator, err := anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
			Digest: f.digest.GetProto(),
		}); err == nil {
			p.Stat = &bazeloutputservice.BatchStatResponse_Stat{
				Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
					File: &bazeloutputservice.BatchStatResponse_Stat_File{
						Locator: locator,
					},
				},
			}
		} else {
			p.Err = status.Error(codes.Internal, "Failed to marshal locator")
		}
	default:
		return false
	}
	return true
}

func (f *blobAccessCASFile) virtualGetAttributesCommon(attributes *Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeRegularFile)
	attributes.SetSizeBytes(uint64(f.digest.GetSizeBytes()))
}

func (f *blobAccessCASFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status) {
	sizeBytes := uint64(f.digest.GetSizeBytes())
	switch regionType {
	case filesystem.Data:
		if offset >= sizeBytes {
			return nil, StatusErrNXIO
		}
		return &offset, StatusOK
	case filesystem.Hole:
		if offset >= sizeBytes {
			return nil, StatusErrNXIO
		}
		return &sizeBytes, StatusOK
	default:
		panic("Requests for other seek modes should have been intercepted")
	}
}

func (f *blobAccessCASFile) VirtualRead(buf []byte, off uint64) (int, bool, Status) {
	size := uint64(f.digest.GetSizeBytes())
	buf, eof := BoundReadToFileSize(buf, off, size)
	if len(buf) > 0 {
		if n, err := f.factory.contentAddressableStorage.Get(f.factory.context, f.digest).ReadAt(buf, int64(off)); n != len(buf) {
			f.factory.errorLogger.Log(util.StatusWrapf(err, "Failed to read from %s at offset %d", f.digest, off))
			return 0, false, StatusErrIO
		}
	}
	return len(buf), eof, StatusOK
}

func (f *blobAccessCASFile) VirtualReadlink(ctx context.Context) ([]byte, Status) {
	return nil, StatusErrInval
}

func (f *blobAccessCASFile) VirtualClose(shareAccess ShareMask) {}

func (f *blobAccessCASFile) virtualSetAttributesCommon(in *Attributes) Status {
	// TODO: chmod() calls against CAS backed files should not be
	// permitted. Unfortunately, we allowed it in the past. When
	// using bb_clientd's Bazel Output Service, we see Bazel
	// performing such calls, so we can't forbid it right now.
	/*
		if _, ok := in.GetPermissions(); ok {
			return StatusErrPerm
		}
	*/
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrAccess
	}
	return StatusOK
}

func (f *blobAccessCASFile) VirtualWrite(buf []byte, off uint64) (int, Status) {
	panic("Request to write to read-only file should have been intercepted")
}

// regularBlobAccessCASFile is the type BlobAccess backed files that are
// not executable (-x).
type regularBlobAccessCASFile struct {
	blobAccessCASFile
}

func (f *regularBlobAccessCASFile) VirtualApply(data any) bool {
	switch p := data.(type) {
	case *ApplyAppendOutputPathPersistencyDirectoryNode:
		p.Directory.Files = append(p.Directory.Files, &remoteexecution.FileNode{
			Name:         p.Name.String(),
			Digest:       f.digest.GetProto(),
			IsExecutable: false,
		})
	default:
		return f.virtualApplyCommon(data)
	}
	return true
}

func (f *regularBlobAccessCASFile) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	f.virtualGetAttributesCommon(attributes)
	attributes.SetPermissions(PermissionsRead)
}

func (f *regularBlobAccessCASFile) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if shareAccess&^ShareMaskRead != 0 || options.Truncate {
		return StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, attributes)
	return StatusOK
}

func (f *regularBlobAccessCASFile) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if s := f.virtualSetAttributesCommon(in); s != StatusOK {
		return s
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}

// regularBlobAccessCASFile is the type BlobAccess backed files that are
// executable (+x).
type executableBlobAccessCASFile struct {
	blobAccessCASFile
}

func (f *executableBlobAccessCASFile) VirtualApply(data any) bool {
	switch p := data.(type) {
	case *ApplyAppendOutputPathPersistencyDirectoryNode:
		p.Directory.Files = append(p.Directory.Files, &remoteexecution.FileNode{
			Name:         p.Name.String(),
			Digest:       f.digest.GetProto(),
			IsExecutable: true,
		})
	default:
		return f.virtualApplyCommon(data)
	}
	return true
}

func (f *executableBlobAccessCASFile) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	f.virtualGetAttributesCommon(attributes)
	attributes.SetPermissions(PermissionsRead | PermissionsExecute)
}

func (f *executableBlobAccessCASFile) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if shareAccess&^ShareMaskRead != 0 || options.Truncate {
		return StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, attributes)
	return StatusOK
}

func (f *executableBlobAccessCASFile) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if s := f.virtualSetAttributesCommon(in); s != StatusOK {
		return s
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}
