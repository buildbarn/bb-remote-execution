package configuration

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/random"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mount of a virtual file system that has been created using
// NewMountFromConfiguration(), but that hasn't been exposed to the
// kernel or network yet. Before calling Expose(), the caller has the
// possibility to construct a root directory.
type Mount interface {
	Expose(rootDirectory virtual.Directory) error
}

type fuseMount struct {
	mountPath       string
	configuration   *pb.FUSEMountConfiguration
	handleAllocator *virtual.FUSEStatefulHandleAllocator
	fsName          string
}

// NewMountFromConfiguration creates a new FUSE mount based on options
// specified in a configuration message and starts processing of
// incoming requests.
func NewMountFromConfiguration(configuration *pb.MountConfiguration, fsName string) (Mount, virtual.StatefulHandleAllocator, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.MountConfiguration_Fuse:
		handleAllocator := virtual.NewFUSEHandleAllocator(random.FastThreadSafeGenerator)
		return &fuseMount{
			mountPath:       configuration.MountPath,
			configuration:   backend.Fuse,
			handleAllocator: handleAllocator,
			fsName:          fsName,
		}, handleAllocator, nil
	default:
		return nil, nil, status.Error(codes.InvalidArgument, "No virtual file system backend configuration provided")
	}
}
