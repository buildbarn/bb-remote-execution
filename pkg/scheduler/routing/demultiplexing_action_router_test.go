package routing_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDemultiplexingActionRouter(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	platformKeyExtractor := mock.NewMockPlatformKeyExtractor(ctrl)
	defaultActionRouter := mock.NewMockActionRouter(ctrl)
	actionRouter := routing.NewDemultiplexingActionRouter(platformKeyExtractor, defaultActionRouter)

	// Register two action router backends.
	linuxPlatform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "os", Value: "linux"},
		},
	}
	linuxActionRouter := mock.NewMockActionRouter(ctrl)
	require.NoError(t, actionRouter.RegisterActionRouter(digest.MustNewInstanceName("a"), linuxPlatform, linuxActionRouter))

	windowsPlatform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "os", Value: "windows"},
		},
	}
	windowsActionRouter := mock.NewMockActionRouter(ctrl)
	require.NoError(t, actionRouter.RegisterActionRouter(digest.MustNewInstanceName("a"), windowsPlatform, windowsActionRouter))

	// Attempting to register another backend with the same instance
	// name prefix and platform properties should fail.
	unusedActionRouter := mock.NewMockActionRouter(ctrl)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.AlreadyExists, "An action router with the same instance name prefix and platform already exists"),
		actionRouter.RegisterActionRouter(digest.MustNewInstanceName("a"), linuxPlatform, unusedActionRouter))

	t.Run("DefaultActionRouter", func(t *testing.T) {
		// Even though the platform properties indicate Linux,
		// the provided REv2 instance name is not a prefix of
		// "a". This means that the request should be sent to
		// the default action router.
		platformKeyExtractor.EXPECT().ExtractKey(ctx, digest.EmptyInstanceName, testutil.EqProto(t, &remoteexecution.Action{})).
			Return(platform.MustNewKey("", linuxPlatform), nil)
		defaultActionRouter.EXPECT().RouteAction(ctx, gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{}), testutil.EqProto(t, &remoteexecution.RequestMetadata{})).
			Return(platform.Key{}, nil, nil, status.Error(codes.Internal, "Got routed to default"))

		_, _, _, err := actionRouter.RouteAction(
			ctx,
			digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
			&remoteexecution.Action{},
			&remoteexecution.RequestMetadata{})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Got routed to default"), err)
	})

	t.Run("LinuxActionRouter", func(t *testing.T) {
		// By setting the REv2 instance name to "a", we should
		// get directed to the Linux action router.
		platformKeyExtractor.EXPECT().ExtractKey(ctx, digest.MustNewInstanceName("a"), testutil.EqProto(t, &remoteexecution.Action{})).
			Return(platform.MustNewKey("a", linuxPlatform), nil)
		linuxActionRouter.EXPECT().RouteAction(ctx, gomock.Any(), testutil.EqProto(t, &remoteexecution.Action{}), testutil.EqProto(t, &remoteexecution.RequestMetadata{})).
			Return(platform.Key{}, nil, nil, status.Error(codes.Internal, "Got routed to Linux"))

		_, _, _, err := actionRouter.RouteAction(
			ctx,
			digest.MustNewFunction("a", remoteexecution.DigestFunction_SHA256),
			&remoteexecution.Action{},
			&remoteexecution.RequestMetadata{})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Got routed to Linux"), err)
	})
}
