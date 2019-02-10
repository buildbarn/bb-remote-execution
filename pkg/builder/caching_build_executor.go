package builder

import (
	"context"
	"fmt"
	"log"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/ac"
	"github.com/buildbarn/bb-storage/pkg/cas"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type cachingBuildExecutor struct {
	base                      BuildExecutor
	contentAddressableStorage cas.ContentAddressableStorage
	actionCache               ac.ActionCache
	browserURL                *url.URL
}

// NewCachingBuildExecutor creates an adapter for BuildExecutor that
// stores action results in the Action Cache (AC) if they may be cached.
// If they may not be cached, they are stored in the Content Addressable
// Storage (CAS) instead.
//
// In both cases, a link to bb_browser is added to the ExecuteResponse,
// so that the user may inspect the Action and ActionResult in detail.
func NewCachingBuildExecutor(base BuildExecutor, contentAddressableStorage cas.ContentAddressableStorage, actionCache ac.ActionCache, browserURL *url.URL) BuildExecutor {
	return &cachingBuildExecutor{
		base:                      base,
		contentAddressableStorage: contentAddressableStorage,
		actionCache:               actionCache,
		browserURL:                browserURL,
	}
}

func (be *cachingBuildExecutor) Execute(ctx context.Context, request *remoteexecution.ExecuteRequest) (*remoteexecution.ExecuteResponse, bool) {
	actionDigest, err := util.NewDigest(request.InstanceName, request.ActionDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to extract digest for action")), false
	}
	response, mayBeCached := be.base.Execute(ctx, request)
	if response.Result == nil {
		// Action ran, but did not yield any results.
		actionURL, err := be.browserURL.Parse(
			fmt.Sprintf(
				"/action/%s/%s/%d/",
				actionDigest.GetInstance(),
				actionDigest.GetHashString(),
				actionDigest.GetSizeBytes()))
		if err != nil {
			log.Fatal(err)
		}
		response.Message = "Action details (no result): " + actionURL.String()
	} else if mayBeCached {
		// Store result in the Action Cache.
		if err := be.actionCache.PutActionResult(ctx, actionDigest, response.Result); err != nil {
			return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to store cached action result")), false
		}

		actionURL, err := be.browserURL.Parse(
			fmt.Sprintf(
				"/action/%s/%s/%d/",
				actionDigest.GetInstance(),
				actionDigest.GetHashString(),
				actionDigest.GetSizeBytes()))
		if err != nil {
			log.Fatal(err)
		}
		response.Message = "Action details (cached result): " + actionURL.String()
	} else {
		// Extension: store the result in the Content
		// Addressable Storage, so the user can at least inspect
		// it through bb_browser.
		uncachedActionResultDigest, err := be.contentAddressableStorage.PutUncachedActionResult(
			ctx,
			&cas_proto.UncachedActionResult{
				ActionDigest: request.ActionDigest,
				ActionResult: response.Result,
			},
			actionDigest)
		if err != nil {
			return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to store uncached action result")), false
		}

		uncachedActionResultURL, err := be.browserURL.Parse(
			fmt.Sprintf(
				"/uncached_action_result/%s/%s/%d/",
				uncachedActionResultDigest.GetInstance(),
				uncachedActionResultDigest.GetHashString(),
				uncachedActionResultDigest.GetSizeBytes()))
		if err != nil {
			log.Fatal(err)
		}
		response.Message = "Action details (uncached result): " + uncachedActionResultURL.String()
	}
	return response, mayBeCached
}
