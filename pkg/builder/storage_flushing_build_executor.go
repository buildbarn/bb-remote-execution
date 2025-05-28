package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// StorageFlusher is a callback that is invoked by
// NewStorageFlushingBuildExecutor to flush contents to storage.
type StorageFlusher func(context.Context) error

type storageFlushingBuildExecutor struct {
	BuildExecutor
	flush StorageFlusher
}

// NewStorageFlushingBuildExecutor is an adapter for BuildExecutor that
// calls a callback after every operation. The callback is typically
// used to flush pending writes to underlying storage, to ensure that
// other processes in the cluster have a consistent view of the
// completion of the operation.
func NewStorageFlushingBuildExecutor(base BuildExecutor, flush StorageFlusher) BuildExecutor {
	return &storageFlushingBuildExecutor{
		BuildExecutor: base,
		flush:         flush,
	}
}

func (be *storageFlushingBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.BuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)
	if err := be.flush(ctx); err != nil {
		attachErrorToExecuteResponse(response, err)

		// Due to flushing failing, some of the outputs
		// referenced by the Execute Response may not be present
		// in the Content Addressable Storage. Even with the
		// error attached to the Execute Response, Bazel will
		// try to access some of the outputs.
		//
		// Prune all digests from the response, as we want Bazel
		// to print the error above, as opposed to print errors
		// related to fetching nonexistent blobs.
		if result := response.Result; result != nil {
			result.OutputFiles = nil
			result.OutputDirectories = nil
			result.StdoutDigest = nil
			result.StderrDigest = nil
		}
		response.ServerLogs = nil
	}
	return response
}
