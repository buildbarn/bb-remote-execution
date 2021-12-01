package platform

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-storage/pkg/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewKeyExtractorFromConfiguration creates a new KeyExtractor based on
// options specified in a configuration file.
func NewKeyExtractorFromConfiguration(configuration *pb.PlatformKeyExtractorConfiguration, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int) (KeyExtractor, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No platform key extractor configuration provided")
	}
	switch kind := configuration.Kind.(type) {
	case *pb.PlatformKeyExtractorConfiguration_Action:
		return ActionKeyExtractor, nil
	case *pb.PlatformKeyExtractorConfiguration_ActionAndCommand:
		return NewActionAndCommandKeyExtractor(contentAddressableStorage, maximumMessageSizeBytes), nil
	case *pb.PlatformKeyExtractorConfiguration_Static:
		return NewStaticKeyExtractor(kind.Static), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported platform key extractor type")
	}
}
