package platform

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// KeyExtractor is responsible for creating a platform key for an
// incoming action execution request, which InMemoryBuildQueue will use
// to determine in which queue an operation is placed.
//
// This interface is used to switch between REv2.1 semantics (reading
// platform properties from the Command message), REv2.2 semantics
// (reading platform properties from the Action message), or to let the
// scheduler rewrite/override platform properties provided by the
// client.
type KeyExtractor interface {
	ExtractKey(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action) (Key, error)
}
