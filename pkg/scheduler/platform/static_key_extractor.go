package platform

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type staticKeyExtractor struct {
	platform *remoteexecution.Platform
}

// NewStaticKeyExtractor creates a KeyExtractor that ignores the
// platform properties provided as part of the action and returns a
// constant value. This implementation is useful in combination with
// DemultiplexingActionRouter, in case rewriting of platform properties
// needs to be performed.
func NewStaticKeyExtractor(platform *remoteexecution.Platform) KeyExtractor {
	return &staticKeyExtractor{
		platform: platform,
	}
}

func (ke *staticKeyExtractor) ExtractKey(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action) (Key, error) {
	return NewKey(instanceName, ke.platform)
}
