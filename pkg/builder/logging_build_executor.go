package builder

import (
	"context"
	"log"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
)

type loggingBuildExecutor struct {
	base       BuildExecutor
	browserURL *url.URL
}

// NewLoggingBuildExecutor wraps an existing BuildExecutor, adding basic
// logging. A link to bb_browser is printed prior to executing the
// action. A JSON representation of the ExecuteResponse is logged after
// completion.
func NewLoggingBuildExecutor(base BuildExecutor, browserURL *url.URL) BuildExecutor {
	return &loggingBuildExecutor{
		base:       base,
		browserURL: browserURL,
	}
}

func (be *loggingBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, instanceName string, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	// Print URL to bb_browser prior to execution.
	if actionDigest, err := util.NewDigest(instanceName, request.ActionDigest); err == nil {
		log.Print("Action: ", re_util.GetBrowserURL(be.browserURL, "action", actionDigest))
	} else {
		log.Print("Action: Failed to extract digest: ", err)
	}

	response := be.base.Execute(ctx, filePool, instanceName, request, executionStateUpdates)

	// Print execution response to log.
	var marshaler jsonpb.Marshaler
	if responseString, err := marshaler.MarshalToString(response); err == nil {
		log.Print("ExecuteResponse: ", responseString)
	} else {
		log.Print("ExecuteResponse: Failed to marshal: ", err)
	}
	return response
}
