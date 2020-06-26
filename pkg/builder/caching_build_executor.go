package builder

import (
	"context"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingBuildExecutor struct {
	base                      BuildExecutor
	contentAddressableStorage cas.ContentAddressableStorage
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
func NewCachingBuildExecutor(base BuildExecutor, contentAddressableStorage cas.ContentAddressableStorage, actionCache blobstore.BlobAccess, browserURL *url.URL) BuildExecutor {
	return &cachingBuildExecutor{
		base:                      base,
		contentAddressableStorage: contentAddressableStorage,
		actionCache:               actionCache,
		browserURL:                browserURL,
	}
}

func (be *cachingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName string, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.base.Execute(ctx, filePool, instanceName, request, executionStateUpdates)
	if actionDigest, err := digest.NewDigestFromPartialDigest(instanceName, request.ActionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
	} else if action := request.Action; action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
	} else if !action.DoNotCache && status.ErrorProto(response.Status) == nil {
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
		if uncachedActionResultDigest, err := be.contentAddressableStorage.PutUncachedActionResult(
			ctx,
			&cas_proto.UncachedActionResult{
				ActionDigest:    actionDigest.GetPartialDigest(),
				ExecuteResponse: response,
			},
			actionDigest); err == nil {
			response.Message = "Action details (uncached result): " + re_util.GetBrowserURL(be.browserURL, "uncached_action_result", uncachedActionResultDigest)
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store uncached action result"))
		}
	}
	return response
}
