package virtual

import (
	"context"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type specialFile struct {
	fileType     filesystem.FileType
	deviceNumber *filesystem.DeviceNumber
}

// NewSpecialFile creates a node that may be used as a character device,
// block device, FIFO or UNIX domain socket. Nodes of these types are
// mere placeholders. The kernel is responsible for capturing calls to
// open() and connect().
func NewSpecialFile(fileType filesystem.FileType, deviceNumber *filesystem.DeviceNumber) NativeLeaf {
	return &specialFile{
		fileType:     fileType,
		deviceNumber: deviceNumber,
	}
}

func (f *specialFile) Link() Status {
	return StatusOK
}

func (f *specialFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *specialFile) Unlink() {
}

func (f *specialFile) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a special file")
}

func (f *specialFile) GetContainingDigests() digest.Set {
	return digest.EmptySet
}

func (f *specialFile) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	return &remoteoutputservice.FileStatus{}, nil
}

func (f *specialFile) AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component) {
	// UNIX sockets or FIFOs do not need to be preserved across
	// restarts of bb_clientd, so there is no need to emit any
	// persistency state.
}

func (f *specialFile) VirtualAllocate(off, size uint64) Status {
	return StatusErrWrongType
}

func (f *specialFile) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	if f.deviceNumber != nil {
		attributes.SetDeviceNumber(*f.deviceNumber)
	}
	attributes.SetFileType(f.fileType)
	attributes.SetLinkCount(StatelessLeafLinkCount)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite)
	attributes.SetSizeBytes(0)
}

func (f *specialFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status) {
	panic("Request to seek on special file should have been intercepted")
}

func (f *specialFile) VirtualOpenSelf(shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	// Even though this file is not a symbolic link, the NFSv4
	// specification requires that NFS4ERR_SYMLINK is returned.
	return StatusErrSymlink
}

func (f *specialFile) VirtualRead(buf []byte, offset uint64) (int, bool, Status) {
	panic("Request to read from special file should have been intercepted")
}

func (f *specialFile) VirtualReadlink() ([]byte, Status) {
	return nil, StatusErrInval
}

func (f *specialFile) VirtualClose() {}

func (f *specialFile) VirtualSetAttributes(in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(requested, out)
	return StatusOK
}

func (f *specialFile) VirtualWrite(buf []byte, off uint64) (int, Status) {
	panic("Request to write to special file should have been intercepted")
}
