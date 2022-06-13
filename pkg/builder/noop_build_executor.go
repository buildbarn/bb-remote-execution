package builder

import (
	"context"
	"net/url"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noopBuildExecutor struct {
	browserURL *url.URL
}

// NewNoopBuildExecutor creates a BuildExecutor that always returns an
// error message when attempting to execute an action.
//
// This implementation may be used to force a build client
// to upload the input root of an action into the Content Addressable
// Storage (CAS) without causing it to be executed afterwards. This may
// be useful when attempting to debug actions.
func NewNoopBuildExecutor(browserURL *url.URL) BuildExecutor {
	return &noopBuildExecutor{
		browserURL: browserURL,
	}
}

func (be *noopBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := NewDefaultExecuteResponse(request)
	if actionDigest, err := instanceName.NewDigestFromProto(request.ActionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
	} else {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Action has been uploaded, but will not be executed. Action details: "+re_util.GetBrowserURL(be.browserURL, "action", actionDigest)))
	}
	return response
}
