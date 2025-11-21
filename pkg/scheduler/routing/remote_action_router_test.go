package routing_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteactionrouter"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
)

func TestRemoteActionRouter(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	remoteClient := mock.NewMockActionRouterClient(ctrl)
	initialSizeClassAnalyzer := mock.NewMockAnalyzer(ctrl)
	actionRouter := routing.NewRemoteActionRouter(remoteClient, initialSizeClassAnalyzer)

	digestFunction := digest.MustNewFunction("prefix", remoteexecution.DigestFunction_SHA256)
	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 123,
		},
	}
	requestMetadata := &remoteexecution.RequestMetadata{
		ToolInvocationId: "build-123",
	}

	t.Run("Success", func(t *testing.T) {
		targetPlatform := &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "arch", Value: "x86_64"},
				{Name: "os", Value: "linux"},
			},
		}
		invocationKey, err := anypb.New(&remoteexecution.RequestMetadata{ToolInvocationId: "build-123"})
		require.NoError(t, err)

		remoteClient.EXPECT().RouteAction(ctx, testutil.EqProto(t, &remoteactionrouter.RouteActionRequest{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			Action:          action,
			RequestMetadata: requestMetadata,
		})).Return(&remoteactionrouter.RouteActionResponse{
			Action:             action,
			InstanceNamePrefix: "target",
			Platform:           targetPlatform,
			InvocationKeys:     []*anypb.Any{invocationKey},
		}, nil)

		mockSelector := mock.NewMockSelector(ctrl)
		initialSizeClassAnalyzer.EXPECT().Analyze(ctx, digestFunction, testutil.EqProto(t, action)).
			Return(mockSelector, nil)

		returnedAction, platformKey, invocationKeys, selector, err := actionRouter.RouteAction(
			ctx,
			digestFunction,
			action,
			requestMetadata)

		require.NoError(t, err)
		require.True(t, testutil.EqProto(t, action).Matches(returnedAction))
		require.Equal(t, platform.MustNewKey("target", targetPlatform), platformKey)
		require.Len(t, invocationKeys, 1)
		expectedKey, err := invocation.NewKey(invocationKey)
		require.NoError(t, err)
		require.Equal(t, expectedKey, invocationKeys[0])
		require.Equal(t, mockSelector, selector)
	})

	t.Run("RemoteServiceError", func(t *testing.T) {
		remoteClient.EXPECT().RouteAction(ctx, testutil.EqProto(t, &remoteactionrouter.RouteActionRequest{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			Action:          action,
			RequestMetadata: requestMetadata,
		})).Return(nil, status.Error(codes.Unavailable, "Remote service unavailable"))

		_, _, _, _, err := actionRouter.RouteAction(
			ctx,
			digestFunction,
			action,
			requestMetadata)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Failed to route action via remote service: Remote service unavailable"),
			err)
	})

	t.Run("CanMutateAction", func(t *testing.T) {
		mutatedAction := &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000000",
				SizeBytes: 456,
			},
		}
		targetPlatform := &remoteexecution.Platform{}

		remoteClient.EXPECT().RouteAction(ctx, testutil.EqProto(t, &remoteactionrouter.RouteActionRequest{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			Action:          action,
			RequestMetadata: requestMetadata,
		})).Return(&remoteactionrouter.RouteActionResponse{
			Action:             mutatedAction,
			InstanceNamePrefix: "",
			Platform:           targetPlatform,
			InvocationKeys:     nil,
		}, nil)

		mockSelector := mock.NewMockSelector(ctrl)
		initialSizeClassAnalyzer.EXPECT().Analyze(ctx, digestFunction, testutil.EqProto(t, mutatedAction)).
			Return(mockSelector, nil)

		returnedAction, _, _, _, err := actionRouter.RouteAction(
			ctx,
			digestFunction,
			action,
			requestMetadata)

		require.NoError(t, err)
		require.Equal(t, mutatedAction, returnedAction)
	})

	t.Run("EmptyActionMeansNoChanges", func(t *testing.T) {
		targetPlatform := &remoteexecution.Platform{}

		remoteClient.EXPECT().RouteAction(ctx, testutil.EqProto(t, &remoteactionrouter.RouteActionRequest{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			Action:          action,
			RequestMetadata: requestMetadata,
		})).Return(&remoteactionrouter.RouteActionResponse{
			Action:             nil,
			InstanceNamePrefix: "",
			Platform:           targetPlatform,
			InvocationKeys:     nil,
		}, nil)

		mockSelector := mock.NewMockSelector(ctrl)
		initialSizeClassAnalyzer.EXPECT().Analyze(ctx, gomock.Any(), gomock.Any()).
			Return(mockSelector, nil)

		returnedAction, _, _, _, err := actionRouter.RouteAction(
			ctx,
			digestFunction,
			action,
			requestMetadata)

		require.NoError(t, err)
		require.Equal(t, action, returnedAction)
	})
}
