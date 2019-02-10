package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type storageFlushingBuildExecutor struct {
	base  BuildExecutor
	flush func(context.Context) error
}

// NewStorageFlushingBuildExecutor is an adapter for BuildExecutor that
// calls a callback after every operation. The callback is typically
// used to flush pending writes to underlying storage, to ensure that
// other processes in the cluster have a consistent view of the
// completion of the operation.
func NewStorageFlushingBuildExecutor(base BuildExecutor, flush func(context.Context) error) BuildExecutor {
	return &storageFlushingBuildExecutor{
		base:  base,
		flush: flush,
	}
}

func (be *storageFlushingBuildExecutor) Execute(ctx context.Context, request *remoteexecution.ExecuteRequest) (*remoteexecution.ExecuteResponse, bool) {
	response, mayBeCached := be.base.Execute(ctx, request)
	if err := be.flush(ctx); err != nil {
		return convertErrorToExecuteResponse(err), false
	}
	return response, mayBeCached
}
