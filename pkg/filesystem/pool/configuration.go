package pool

import (
	"math"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewFilepoolParametersFromConfiguration constructs the input variables
// required for constructing a FilePool (a block device, a sector
// allocator and the size of each sector) based on parameters provided
// in a configuration file.
//
// The file pool object itself must be constructed by the caller as the
// caller may want to add additional decorators on top of the allocator.
func NewFilepoolParametersFromConfiguration(configuration *pb.FilePoolConfiguration) (blockdevice.BlockDevice, filesystem.SectorAllocator, int, error) {
	if configuration == nil {
		// No configuration provided. Because there are setups
		// in which it's not required to use a file pool, let's
		// return empty parameters by default.
		return nil, nil, 0, nil
	}

	var allocator filesystem.SectorAllocator
	var blockDevice blockdevice.BlockDevice
	var sectorSizeBytes int
	switch backend := configuration.Backend.(type) {
	case *pb.FilePoolConfiguration_BlockDevice:
		var sectorCount int64
		var err error
		blockDevice, sectorSizeBytes, sectorCount, err = blockdevice.NewBlockDeviceFromConfiguration(backend.BlockDevice, true)
		if err != nil {
			return nil, nil, 0, util.StatusWrap(err, "Failed to create block device")
		}
		if sectorCount > math.MaxUint32 {
			return nil, nil, 0, util.StatusWrapf(err, "Block device has %d sectors, while only %d may be addressed", sectorCount, uint32(math.MaxUint32))
		}
		allocator = filesystem.NewBitmapSectorAllocator(uint32(sectorCount))
	default:
		return nil, nil, 0, status.Error(codes.InvalidArgument, "Configuration did not contain a supported file pool backend")
	}
	return blockDevice, allocator, sectorSizeBytes, nil
}
