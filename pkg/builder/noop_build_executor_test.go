package builder_test

import (
	"context"
	"net/url"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNoopBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildExecutor := builder.NewNoopBuildExecutor(
		contentAddressableStorage,
		/* maximumMessageSizeBytes = */ 10000,
		&url.URL{
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
				digest.MustNewFunction("build", remoteexecution.DigestFunction_MD5),
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
				Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: Hash has length 24, while 32 characters were expected").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewFunction("build", remoteexecution.DigestFunction_MD5),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "This is not a valid hash",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "0d47d8528c97bdc9862674e6a8090cad",
							SizeBytes: 1200,
						},
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})

	t.Run("InvalidTemplate", func(t *testing.T) {
		// If an invalid template is provided in the
		// environment, parsing it should fail.
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("build", remoteexecution.DigestFunction_SHA256, "7f53aed4b5489c487be514dd88d3314d966e19b84bc766a972d82246ee6f494f", 150)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{
						Name:  "NOOP_WORKER_ERROR_MESSAGE_TEMPLATE",
						Value: "{{ foobarbaz }}",
					},
				},
			}, buffer.UserProvided))
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Invalid error message template: template: NOOP_WORKER_ERROR_MESSAGE_TEMPLATE:1: function \"foobarbaz\" not defined").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewFunction("build", remoteexecution.DigestFunction_SHA256),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "4e3fbcd2916efea4cc61d57b4a097df2b467e6b2207f6e242457a8705c5dc689",
						SizeBytes: 234,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "7f53aed4b5489c487be514dd88d3314d966e19b84bc766a972d82246ee6f494f",
							SizeBytes: 150,
						},
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})

	t.Run("SuccessDefaultTemplate", func(t *testing.T) {
		// If no template is provided in the environment
		// variables, then a default template should be used.
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("build", remoteexecution.DigestFunction_SHA256, "d134371fd7573f7ef77c90e907c8bfaf95f34b82ac8503dbed5e062fb6fe4702", 200)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{}, buffer.UserProvided))
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Action has been uploaded, but will not be executed. Action details: http://example.com/some/sub/directory/build/blobs/sha256/action/4b3f66e160293a393a1fe2dd13721368c944d949e11f97985a893a5b76877346-123/").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewFunction("build", remoteexecution.DigestFunction_SHA256),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "4b3f66e160293a393a1fe2dd13721368c944d949e11f97985a893a5b76877346",
						SizeBytes: 123,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "d134371fd7573f7ef77c90e907c8bfaf95f34b82ac8503dbed5e062fb6fe4702",
							SizeBytes: 200,
						},
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})

	t.Run("SuccessCustomTemplate", func(t *testing.T) {
		// If a custom template is provided in the environment,
		// it should be preferred over the default template.
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("build", remoteexecution.DigestFunction_SHA256, "9da17cb226048f5bb3e6a20311b551e73ce8ac0d408e69e737d28a8f3179d0ce", 300)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{
						Name:  "PATH",
						Value: "/bin:/sbin:/usr/bin:/usr/sbin",
					},
					{
						Name:  "NOOP_WORKER_ERROR_MESSAGE_TEMPLATE",
						Value: "Please visit {{ .ActionURL }} to inspect the action",
					},
				},
			}, buffer.UserProvided))
		filePool := mock.NewMockFilePool(ctrl)
		testutil.RequireEqualProto(
			t,
			&remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
				},
				Status: status.New(codes.InvalidArgument, "Please visit http://example.com/some/sub/directory/build/blobs/sha256/action/e1497d75baa26b50b71b14086d1553586e817385670ac550f36b94cd50b8e25d-456/ to inspect the action").Proto(),
			},
			buildExecutor.Execute(
				ctx,
				filePool,
				digest.MustNewFunction("build", remoteexecution.DigestFunction_SHA256),
				&remoteworker.DesiredState_Executing{
					ActionDigest: &remoteexecution.Digest{
						Hash:      "e1497d75baa26b50b71b14086d1553586e817385670ac550f36b94cd50b8e25d",
						SizeBytes: 456,
					},
					Action: &remoteexecution.Action{
						CommandDigest: &remoteexecution.Digest{
							Hash:      "9da17cb226048f5bb3e6a20311b551e73ce8ac0d408e69e737d28a8f3179d0ce",
							SizeBytes: 300,
						},
					},
				},
				make(chan *remoteworker.CurrentState_Executing, 10)))
	})
}
