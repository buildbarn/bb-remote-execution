package virtual

import (
	"context"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type specialFile struct {
	placeholderFile

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

func (f *specialFile) Readlink() (string, error) {
	return "", syscall.EINVAL
}

func (f *specialFile) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	return &remoteoutputservice.FileStatus{}, nil
}

func (f *specialFile) AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component) {
	// UNIX sockets or FIFOs do not need to be preserved across
	// restarts of bb_clientd, so there is no need to emit any
	// persistency state.
}

func (f *specialFile) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	if f.deviceNumber != nil {
		attributes.SetDeviceNumber(*f.deviceNumber)
	}
	attributes.SetFileType(f.fileType)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite)
	attributes.SetSizeBytes(0)
}

func (f *specialFile) VirtualReadlink(ctx context.Context) ([]byte, Status) {
	return nil, StatusErrInval
}

func (f *specialFile) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}
