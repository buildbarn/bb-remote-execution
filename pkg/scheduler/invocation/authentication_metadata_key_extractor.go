package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"

	"google.golang.org/protobuf/types/known/anypb"
)

type authenticationMetadataKeyExtractor struct{}

func (ke authenticationMetadataKeyExtractor) ExtractKey(ctx context.Context, requestMetadata *remoteexecution.RequestMetadata) (Key, error) {
	authenticationMetadata, _ := auth.AuthenticationMetadataFromContext(ctx).GetPublicProto()
	any, err := anypb.New(authenticationMetadata)
	if err != nil {
		return "", err
	}
	return NewKey(any)
}

// AuthenticationMetadataKeyExtractor is an implementation of
// KeyExtractor that returns a Key that is based on the publicly
// displayable part of the authentication metadata. This will cause
// InMemoryBuildQueue to group all operations created by the same user
// together, which ensures fair scheduling between users.
var AuthenticationMetadataKeyExtractor KeyExtractor = authenticationMetadataKeyExtractor{}
