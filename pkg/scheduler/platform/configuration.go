package platform

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"

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
	case *pb.PlatformKeyExtractorConfiguration_PropertyFiltering:
		config := kind.PropertyFiltering
		base, err := NewKeyExtractorFromConfiguration(config.KeyExtractor, contentAddressableStorage, maximumMessageSizeBytes)
		if err != nil {
			return nil, util.StatusWrap(err, "Creating base keyExtractor")
		}
		if len(config.KeepKeys) > 0 && len(config.DiscardKeys) > 0 {
			return nil, status.Error(codes.InvalidArgument, "Cannot specify both keep and discard keys")
		}
		if len(config.KeepKeys) > 0 {
			return NewPropertyFilteringKeyExtractor(base, config.KeepKeys, false), nil
		} else if len(config.DiscardKeys) > 0 {
			return NewPropertyFilteringKeyExtractor(base, config.DiscardKeys, true), nil
		}
		return nil, status.Error(codes.InvalidArgument, "Must specify keys to keep or discard")
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported platform key extractor type")
	}
}
