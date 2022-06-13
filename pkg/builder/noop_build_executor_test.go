package builder_test

import (
	"context"
	"net/url"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNoopBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildExecutor := builder.NewNoopBuildExecutor(&url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	t.Run("NoActionDigest", func(t *testing.T) {
		// The client needs to provide an Action digest, so that
		// this BuildExecutor can generate a link to bb_browser.
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: No digest provided").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewInstanceName("build"),
				&remoteworker.DesiredState_Executing{},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})

	t.Run("InvalidActionDigest", func(t *testing.T) {
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: Unknown digest hash length: 24 characters").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewInstanceName("build"),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "This is not a valid hash",
						SizeBytes: 123,
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})

	t.Run("ValidActionDigest", func(t *testing.T) {
		// Successful processing of the Action.
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Action has been uploaded, but will not be executed. Action details: http://example.com/some/sub/directory/build/blobs/action/4b3f66e160293a393a1fe2dd13721368c944d949e11f97985a893a5b76877346-123/").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewInstanceName("build"),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "4b3f66e160293a393a1fe2dd13721368c944d949e11f97985a893a5b76877346",
						SizeBytes: 123,
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})
}
