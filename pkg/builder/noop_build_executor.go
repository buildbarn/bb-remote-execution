package builder

import (
	"context"
	"net/url"
	"strings"
	"text/template"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noopBuildExecutor struct {
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
	browserURL                *url.URL
}

// NewNoopBuildExecutor creates a BuildExecutor that always returns an
// error message when attempting to execute an action.
//
// This implementation may be used to force a build client
// to upload the input root of an action into the Content Addressable
// Storage (CAS) without causing it to be executed afterwards. This may
// be useful when attempting to debug actions.
func NewNoopBuildExecutor(contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int, browserURL *url.URL) BuildExecutor {
	return &noopBuildExecutor{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		browserURL:                browserURL,
	}
}

func (be *noopBuildExecutor) CheckReadiness(ctx context.Context) error {
	return nil
}

var defaultNoopErrorMessageTemplate = template.Must(
	template.New("NoopBuildExecutor").
		Parse("Action has been uploaded, but will not be executed. Action details: {{ .ActionURL }}"))

func (be *noopBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	// Obtain action digest, which can be embedded in the error message.
	response := NewDefaultExecuteResponse(request)
	actionDigest, err := digestFunction.NewDigestFromProto(request.ActionDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
		return response
	}

	// Extract the error message template from the Command message.
	action := request.Action
	if action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	commandDigest, err := digestFunction.NewDigestFromProto(request.Action.CommandDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for command"))
		return response
	}
	commandMessage, err := be.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, be.maximumMessageSizeBytes)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to obtain command"))
		return response
	}
	command := commandMessage.(*remoteexecution.Command)

	errorMessageTemplate := defaultNoopErrorMessageTemplate
	for _, environmentVariable := range command.EnvironmentVariables {
		if environmentVariable.Name == "NOOP_WORKER_ERROR_MESSAGE_TEMPLATE" {
			userProvidedTemplate, err := template.New(environmentVariable.Name).Parse(environmentVariable.Value)
			if err != nil {
				attachErrorToExecuteResponse(response, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid error message template"))
				return response
			}
			errorMessageTemplate = userProvidedTemplate
		}
	}

	// Generate error message to return.
	var errorMessage strings.Builder
	if err := errorMessageTemplate.Execute(&errorMessage, struct {
		ActionURL string
	}{
		ActionURL: re_util.GetBrowserURL(be.browserURL, "action", actionDigest),
	}); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot evaluate error message template"))
		return response
	}

	attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, errorMessage.String()))
	return response
}
