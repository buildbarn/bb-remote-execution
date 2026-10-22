package platform

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

type strippingKeyExtractor struct {
	base          KeyExtractor
	propertyNames map[string]struct{}
}

// NewStrippingKeyExtractor creates a decorator for KeyExtractor that
// removes a given set of REv2 platform properties from the action
// before the platform key is computed.
//
// Workers announce the platform properties they provide to the
// scheduler, and InMemoryBuildQueue only assigns actions to a worker if
// the resulting platform keys are identical. This means that platform
// properties that merely act as a hint to the worker, as opposed to
// describing a capability of the execution environment, would cause
// actions to remain queued indefinitely.
//
// The most notable example of such a property is 'persistentWorkerKey',
// which Bazel sets to a fingerprint of the tool that needs to execute
// the action when --experimental_remote_mark_tool_inputs is used. As
// this value differs for every tool, and is not known by workers ahead
// of time, it must be stripped.
//
// Note that this only affects the routing decision. The action that is
// sent to the worker remains unmodified, meaning the worker can still
// observe the properties that are stripped here.
func NewStrippingKeyExtractor(base KeyExtractor, propertyNames []string) KeyExtractor {
	propertyNamesMap := make(map[string]struct{}, len(propertyNames))
	for _, propertyName := range propertyNames {
		propertyNamesMap[propertyName] = struct{}{}
	}
	return &strippingKeyExtractor{
		base:          base,
		propertyNames: propertyNamesMap,
	}
}

func (ke *strippingKeyExtractor) ExtractKey(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (Key, error) {
	// Determine whether any of the properties that need to be
	// stripped are present. Most actions don't have any, in which
	// case we can forward the original action.
	needsStripping := false
	for _, property := range action.GetPlatform().GetProperties() {
		if _, ok := ke.propertyNames[property.Name]; ok {
			needsStripping = true
			break
		}
	}
	if !needsStripping {
		return ke.base.ExtractKey(ctx, digestFunction, action)
	}

	// Remove the properties from a copy of the action, so that the
	// caller continues to observe the original set of properties.
	strippedAction := proto.Clone(action).(*remoteexecution.Action)
	properties := strippedAction.Platform.Properties
	strippedProperties := properties[:0]
	for _, property := range properties {
		if _, ok := ke.propertyNames[property.Name]; !ok {
			strippedProperties = append(strippedProperties, property)
		}
	}
	strippedAction.Platform.Properties = strippedProperties
	return ke.base.ExtractKey(ctx, digestFunction, strippedAction)
}
