package builder

import (
	"context"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	cas_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingBuildExecutor struct {
	BuildExecutor
	chunkStorage         blobstore.BlobAccess[*chunk.Chunk]
	chunkMappingStorage  blobstore.BlobAccess[chunk.Mapping]
	cdcParametersFetcher capabilities.CDCParametersFetcher
	zstdPool             zstd.Pool
	actionCache          blobstore.BlobAccess[*remoteexecution.ActionResult]
	portalURL            *url.URL
}

// NewCachingBuildExecutor creates an adapter for BuildExecutor that
// stores action results in the Action Cache (AC) if they may be cached.
// If they may not be cached, they are stored in the Content Addressable
// Storage (CAS) instead.
//
// In both cases, a link to bb-portal is added to the ExecuteResponse,
// so that the user may inspect the Action and ActionResult in detail.
func NewCachingBuildExecutor(base BuildExecutor, chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], cdcParametersFetcher capabilities.CDCParametersFetcher, zstdPool zstd.Pool, actionCache blobstore.BlobAccess[*remoteexecution.ActionResult], portalURL *url.URL) BuildExecutor {
	return &cachingBuildExecutor{
		BuildExecutor:        base,
		chunkStorage:         chunkStorage,
		chunkMappingStorage:  chunkMappingStorage,
		cdcParametersFetcher: cdcParametersFetcher,
		zstdPool:             zstdPool,
		actionCache:          actionCache,
		portalURL:            portalURL,
	}
}

func (be *cachingBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.BuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)
	if actionDigest, err := digestFunction.NewDigestFromProto(request.ActionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
	} else if action := request.Action; action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
	} else if !action.DoNotCache && executeResponseIsSuccessful(response) {
		// Store result in the Action Cache.
		if err := be.actionCache.Put(ctx, actionDigest, response.Result); err == nil {
			response.Message = "Action details (cached result): " + re_util.GetPortalURL(be.portalURL, "action", actionDigest)
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store cached action result"))
		}
	} else {
		// Extension: store the result in the Content
		// Addressable Storage, so the user can at least inspect
		// it through bb_portal.
		params, err := be.cdcParametersFetcher.FetchCDCParameters(ctx, actionDigest.GetInstanceName())
		if err != nil {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to fetch CDC parameters"))
			return response
		}
		if historicalExecuteResponseDigest, err := re_cas.PutProto(
			ctx,
			be.zstdPool,
			be.chunkStorage,
			be.chunkMappingStorage,
			params,
			&cas_proto.HistoricalExecuteResponse{
				ActionDigest:    actionDigest.GetProto(),
				ExecuteResponse: response,
			},
			actionDigest.GetDigestFunction(),
		); err == nil {
			response.Message = "Action details (uncached result): " + re_util.GetPortalURL(be.portalURL, "historical_execute_response", historicalExecuteResponseDigest)
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store historical execute response"))
		}
	}
	return response
}
