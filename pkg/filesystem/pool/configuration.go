package pool

import (
	"math"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FilePoolFactory allows constructions of configured file pools with
// their own SectorAllocators. This is required to manage quotas for
// different file pools.
type FilePoolFactory struct {
	blockDevice     blockdevice.BlockDevice
	sectorSizeBytes int
}

// NewFilePool creates the file pool for the BlockDevice managed by
// FilePoolFactory. To allow different quotas at the SectorAllocator
// level for the different FilePools the function takes an allocator
// that will ultimately allocate from the same block device as other
// FilePools created by this FilePoolFactory.
func (f FilePoolFactory) NewFilePool(allocator SectorAllocator) FilePool {
	return NewBlockDeviceBackedFilePool(f.blockDevice, allocator, f.sectorSizeBytes)
}

// NewFilePoolFactoryFromConfiguration constructs a file pool factory
// and a shared sector allocator for the file pool based on parameters
// provided in a configuration file.
//
// The file pool object itself must be constructed by calling the the
// factory with the allocator. This is done as the caller may want to
// add additional decorators on top of the allocator.
func NewFilePoolFactoryFromConfiguration(configuration *pb.FilePoolConfiguration) (FilePoolFactory, SectorAllocator, int, error) {
	if configuration == nil {
		// No configuration provided. Because there are setups
		// in which it's not required to use a file pool, let's
		// return a nil factory by default.
		return FilePoolFactory{}, nil, 0, nil
	}

	var allocator SectorAllocator
	var factory FilePoolFactory
	var sectorSizeBytes int
	switch backend := configuration.Backend.(type) {
	case *pb.FilePoolConfiguration_BlockDevice:
		var blockDevice blockdevice.BlockDevice
		var sectorCount int64
		var err error
		blockDevice, sectorSizeBytes, sectorCount, err = blockdevice.NewBlockDeviceFromConfiguration(backend.BlockDevice, true)
		if err != nil {
			return FilePoolFactory{}, nil, 0, util.StatusWrap(err, "Failed to create block device")
		}
		if sectorCount > math.MaxUint32 {
			return FilePoolFactory{}, nil, 0, util.StatusWrapf(err, "Block device has %d sectors, while only %d may be addressed", sectorCount, uint32(math.MaxUint32))
		}
		allocator = NewBitmapSectorAllocator(uint32(sectorCount))
		factory = FilePoolFactory{
			blockDevice:     blockDevice,
			sectorSizeBytes: sectorSizeBytes,
		}
	default:
		return FilePoolFactory{}, nil, 0, status.Error(codes.InvalidArgument, "Configuration did not contain a supported file pool backend")
	}
	return factory, allocator, sectorSizeBytes, nil
}
