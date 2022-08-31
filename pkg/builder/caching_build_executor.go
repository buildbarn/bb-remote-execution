package builder

import (
	"context"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingBuildExecutor struct {
	BuildExecutor
	contentAddressableStorage blobstore.BlobAccess
	actionCache               blobstore.BlobAccess
	browserURL                *url.URL
}

// NewCachingBuildExecutor creates an adapter for BuildExecutor that
// stores action results in the Action Cache (AC) if they may be cached.
// If they may not be cached, they are stored in the Content Addressable
// Storage (CAS) instead.
//
// In both cases, a link to bb_browser is added to the ExecuteResponse,
// so that the user may inspect the Action and ActionResult in detail.
func NewCachingBuildExecutor(base BuildExecutor, contentAddressableStorage, actionCache blobstore.BlobAccess, browserURL *url.URL) BuildExecutor {
	return &cachingBuildExecutor{
		BuildExecutor:             base,
		contentAddressableStorage: contentAddressableStorage,
		actionCache:               actionCache,
		browserURL:                browserURL,
	}
}

func (be *cachingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.BuildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates)
	if actionDigest, err := instanceName.NewDigestFromProto(request.ActionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
	} else if action := request.Action; action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
	} else if !action.DoNotCache && status.ErrorProto(response.Status) == nil && response.Result.ExitCode == 0 {
		// Store result in the Action Cache.
		if err := be.actionCache.Put(ctx, actionDigest, buffer.NewProtoBufferFromProto(response.Result, buffer.UserProvided)); err == nil {
			response.Message = "Action details (cached result): " + re_util.GetBrowserURL(be.browserURL, "action", actionDigest)
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store cached action result"))
		}
	} else {
		// Extension: store the result in the Content
		// Addressable Storage, so the user can at least inspect
		// it through bb_browser.
		if historicalExecuteResponseDigest, err := blobstore.CASPutProto(
			ctx,
			be.contentAddressableStorage,
			&cas_proto.HistoricalExecuteResponse{
				ActionDigest:    actionDigest.GetProto(),
				ExecuteResponse: response,
			},
			actionDigest.GetDigestFunction()); err == nil {
			response.Message = "Action details (uncached result): " + re_util.GetBrowserURL(be.browserURL, "historical_execute_response", historicalExecuteResponseDigest)
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store historical execute response"))
		}
	}
	return response
}
