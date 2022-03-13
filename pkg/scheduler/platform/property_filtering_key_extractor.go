package platform

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
)

type propertyFilteringKeyExtractor struct {
	base    KeyExtractor
	keys    map[string]bool
	discard bool
}

// PropertyFilteringKeyExtractor takes a platform extracted by another
// KeyExtractor, such as ActionKeyExtractor, and mutates it by either discarding
// or only retaining specified keys.  When `discard` is false, it acts like an
// allowlist, only retaining platform properties whose name appears in `keys`.
// Whe `discard` is true, any of `keys` that appear in platform properties are
// removed from the platform.
func NewPropertyFilteringKeyExtractor(base KeyExtractor, keys []string, discard bool) KeyExtractor {
	keyMap := make(map[string]bool)
	for _, item := range keys {
		keyMap[item] = true
	}
	return &propertyFilteringKeyExtractor{
		base:    base,
		keys:    keyMap,
		discard: discard,
	}
}

func (pf propertyFilteringKeyExtractor) ExtractKey(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action) (Key, error) {
	key, err := pf.base.ExtractKey(ctx, instanceName, action)
	if err != nil {
		return key, err
	}

	platform := &remoteexecution.Platform{}

	err = jsonpb.UnmarshalString(key.GetPlatformString(), platform)
	if err != nil {
		return key, util.StatusWrap(err, "Failed decoding platform")
	}

	newPlatform := &remoteexecution.Platform{}

	for _, property := range platform.Properties {
		if _, ok := pf.keys[property.Name]; ok != pf.discard {
			newPlatform.Properties = append(newPlatform.Properties, property)
		}
	}

	return NewKey(key.GetInstanceNamePrefix(), newPlatform)
}
