package invocation

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/types/known/anypb"
)

type platformPropertyKeyExtractor struct {
	propertyName string
}

// NewPlatformPropertyKeyExtractor creates a KeyExtractor that returns a
// Key that is based on the value of a single REv2 platform property.
// Actions that don't have a property with the provided name are grouped
// together as if the property had an empty value.
//
// This implementation is intended to be used in combination with
// InMemoryBuildQueue's worker invocation stickiness. By grouping
// operations by the 'persistentWorkerKey' platform property that Bazel
// sets when --experimental_remote_mark_tool_inputs is used, workers
// preferably keep executing actions that can be handled by a persistent
// worker process that they already launched.
func NewPlatformPropertyKeyExtractor(propertyName string) KeyExtractor {
	return &platformPropertyKeyExtractor{
		propertyName: propertyName,
	}
}

func (ke *platformPropertyKeyExtractor) ExtractKey(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (Key, error) {
	value := ""
	for _, property := range action.GetPlatform().GetProperties() {
		if property.Name == ke.propertyName {
			value = property.Value
			break
		}
	}
	any, err := anypb.New(&buildqueuestate.PlatformProperty{
		Name:  ke.propertyName,
		Value: value,
	})
	if err != nil {
		return "", err
	}
	return NewKey(any)
}
