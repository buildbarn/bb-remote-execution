package runner_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAppleXcodeResolvingRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseRunner := mock.NewMockRunnerServer(ctrl)
	sdkRootResolver := mock.NewMockAppleXcodeSDKRootResolver(ctrl)
	runner := runner.NewAppleXcodeResolvingRunner(
		baseRunner,
		map[string]string{
			"13.4.1.13F100": "/Applications/Xcode13.app/Contents/Developer",
			"14.2.0.14C18":  "/Applications/Xcode14.app/Contents/Developer",
		},
		sdkRootResolver.Call)

	response := &runner_pb.RunResponse{
		ExitCode: 123,
	}

	t.Run("NoXcodeVersionSet", func(t *testing.T) {
		// This decorator should not have any effect if no Xcode
		// version is provided.
		request := &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"PATH": "/bin:/usr/bin:/usr/local/bin",
			},
		}
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).Return(response, nil)

		observedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})

	t.Run("DeveloperDirAlreadySet", func(t *testing.T) {
		// If DEVELOPER_DIR is already set, this decorator should have
		// no effect.
		request := &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"DEVELOPER_DIR":          "/some/path",
				"XCODE_VERSION_OVERRIDE": "13.4.1.13F100",
			},
		}
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).Return(response, nil)

		observedResponse, err := runner.Run(ctx, request)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})

	t.Run("InvalidXcodeVersion", func(t *testing.T) {
		// Execution requests that use an unknown version of
		// Xcode should be rejected.
		_, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"XCODE_VERSION_OVERRIDE": "12.5.1.12E507",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Attempted to use Xcode installation with version \"12.5.1.12E507\", while only [13.4.1.13F100 14.2.0.14C18] are supported"), err)
	})

	t.Run("OnlyDeveloperDir", func(t *testing.T) {
		// If only XCODE_VERSION_OVERRIDE is set without providing
		// APPLE_SDK_PLATFORM and APPLE_SDK_VERSION_OVERRIDE, we
		// can only set DEVELOPER_DIR. SDKROOT cannot be set.
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"DEVELOPER_DIR":          "/Applications/Xcode13.app/Contents/Developer",
				"XCODE_VERSION_OVERRIDE": "13.4.1.13F100",
			},
		})).Return(response, nil)

		observedResponse, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"XCODE_VERSION_OVERRIDE": "13.4.1.13F100",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})

	t.Run("SDKRootAlreadySet", func(t *testing.T) {
		// If SDKROOT is set, we should not overwrite it.
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"APPLE_SDK_PLATFORM":         "MacOSX",
				"APPLE_SDK_VERSION_OVERRIDE": "13.1",
				"DEVELOPER_DIR":              "/Applications/Xcode13.app/Contents/Developer",
				"SDKROOT":                    "/some/path",
				"XCODE_VERSION_OVERRIDE":     "13.4.1.13F100",
			},
		})).Return(response, nil)

		observedResponse, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"APPLE_SDK_PLATFORM":         "MacOSX",
				"APPLE_SDK_VERSION_OVERRIDE": "13.1",
				"SDKROOT":                    "/some/path",
				"XCODE_VERSION_OVERRIDE":     "13.4.1.13F100",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})

	t.Run("UnknownSDK", func(t *testing.T) {
		// Errors resolving the root of the SDK should be propagated.
		sdkRootResolver.EXPECT().Call(ctx, "/Applications/Xcode13.app/Contents/Developer", "macosx13.1").
			Return("", status.Error(codes.FailedPrecondition, "SDK not found"))

		_, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"APPLE_SDK_PLATFORM":         "MacOSX",
				"APPLE_SDK_VERSION_OVERRIDE": "13.1",
				"XCODE_VERSION_OVERRIDE":     "13.4.1.13F100",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Cannot resolve root for SDK \"macosx13.1\" in Xcode developer directory \"/Applications/Xcode13.app/Contents/Developer\": SDK not found"), err)
	})

	t.Run("BothDeveloperDirAndSDKRoot", func(t *testing.T) {
		// Example invocation where both DEVELOPER_DIR and
		// SDKROOT end up getting set.
		sdkRootResolver.EXPECT().Call(ctx, "/Applications/Xcode13.app/Contents/Developer", "macosx13.1").
			Return("/Applications/Xcode13.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX13.1.sdk", nil)
		baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"APPLE_SDK_PLATFORM":         "MacOSX",
				"APPLE_SDK_VERSION_OVERRIDE": "13.1",
				"DEVELOPER_DIR":              "/Applications/Xcode13.app/Contents/Developer",
				"SDKROOT":                    "/Applications/Xcode13.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX13.1.sdk",
				"XCODE_VERSION_OVERRIDE":     "13.4.1.13F100",
			},
		})).Return(response, nil)

		observedResponse, err := runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"cc", "-o", "hello.o", "hello.c"},
			EnvironmentVariables: map[string]string{
				"APPLE_SDK_PLATFORM":         "MacOSX",
				"APPLE_SDK_VERSION_OVERRIDE": "13.1",
				"XCODE_VERSION_OVERRIDE":     "13.4.1.13F100",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse)
	})
}
