package filesystem

import (
	"math"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewFilePoolFromConfiguration constructs a FilePool based on
// parameters provided in a configuration file.
func NewFilePoolFromConfiguration(configuration *pb.FilePoolConfiguration) (FilePool, error) {
	if configuration == nil {
		// No configuration provided. Because there are setups
		// in which it's not required to use a file pool, let's
		// return an empty file pool by default.
		return EmptyFilePool, nil
	}

	switch backend := configuration.Backend.(type) {
	case *pb.FilePoolConfiguration_InMemory:
		return InMemoryFilePool, nil
	case *pb.FilePoolConfiguration_DirectoryPath:
		directory, err := filesystem.NewLocalDirectory(backend.DirectoryPath)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to open directory %#v", backend.DirectoryPath)
		}
		if err := directory.RemoveAllChildren(); err != nil {
			directory.Close()
			return nil, util.StatusWrapf(err, "Failed to empty out directory %#v", backend.DirectoryPath)
		}
		return NewDirectoryBackedFilePool(directory), nil
	case *pb.FilePoolConfiguration_BlockDevice:
		blockDevice, sectorSizeBytes, sectorCount, err := blockdevice.NewBlockDeviceFromConfiguration(backend.BlockDevice, true)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create block device")
		}
		if sectorCount > math.MaxUint32 {
			return nil, util.StatusWrapf(err, "Block device has %d sectors, while only %d may be addressed", sectorCount, uint32(math.MaxUint32))
		}
		return NewBlockDeviceBackedFilePool(
			blockDevice,
			NewBitmapSectorAllocator(uint32(sectorCount)),
			sectorSizeBytes), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported file pool backend")
	}
}
