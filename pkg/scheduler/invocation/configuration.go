package invocation

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewKeyExtractorFromConfiguration creates a KeyExtractor based on
// settings provided in a configuration file.
func NewKeyExtractorFromConfiguration(configuration *pb.InvocationKeyExtractorConfiguration) (KeyExtractor, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No invocation key extractor coniguration provided")
	}
	switch configuration.Kind.(type) {
	case *pb.InvocationKeyExtractorConfiguration_ToolInvocationId:
		return ToolInvocationIDKeyExtractor, nil
	case *pb.InvocationKeyExtractorConfiguration_CorrelatedInvocationsId:
		return CorrelatedInvocationsIDKeyExtractor, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported invocation key extractor type")
	}
}
