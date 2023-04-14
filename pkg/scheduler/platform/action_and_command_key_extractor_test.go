package platform_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActionAndCommandKeyExtractor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	keyExtractor := platform.NewActionAndCommandKeyExtractor(contentAddressableStorage, 1024*1024)
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)

	t.Run("ActionInvalidProperties", func(t *testing.T) {
		_, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "os", Value: "linux"},
					{Name: "arch", Value: "aarch64"},
				},
			},
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to extract platform key from action: Platform properties are not lexicographically sorted, as property "), err)
	})

	t.Run("ActionSuccess", func(t *testing.T) {
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "arch", Value: "aarch64"},
					{Name: "os", Value: "linux"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "arch", Value: "aarch64"},
					{Name: "os", Value: "linux"},
				},
			},
		}, key.GetPlatformQueueName())
	})

	t.Run("CommandInvalidDigest", func(t *testing.T) {
		_, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "4216455ceebbc3038bd0550c85b6a3bf",
				SizeBytes: -1,
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to extract digest for command: Invalid digest size: -1 bytes"), err)
	})

	t.Run("CommandStorageFailure", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4216455ceebbc3038bd0550c85b6a3bf", 123)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Cannot establish network connection")))

		_, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "4216455ceebbc3038bd0550c85b6a3bf",
				SizeBytes: 123,
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to obtain command: Cannot establish network connection"), err)
	})

	t.Run("CommandInvalidProperties", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4216455ceebbc3038bd0550c85b6a3bf", 123)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						{Name: "os", Value: "linux"},
						{Name: "arch", Value: "aarch64"},
					},
				},
			}, buffer.UserProvided))

		_, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "4216455ceebbc3038bd0550c85b6a3bf",
				SizeBytes: 123,
			},
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to extract platform key from command: Platform properties are not lexicographically sorted, as property "), err)
	})

	t.Run("CommandSuccess", func(t *testing.T) {
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4216455ceebbc3038bd0550c85b6a3bf", 123)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						{Name: "arch", Value: "aarch64"},
						{Name: "os", Value: "linux"},
					},
				},
			}, buffer.UserProvided))

		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "4216455ceebbc3038bd0550c85b6a3bf",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "arch", Value: "aarch64"},
					{Name: "os", Value: "linux"},
				},
			},
		}, key.GetPlatformQueueName())
	})

	t.Run("NoPlatformPresent", func(t *testing.T) {
		// If no platform object is, assume the empty set of
		// platform properties. Clients such as BuildStream are
		// known for not providing them.
		contentAddressableStorage.EXPECT().Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4216455ceebbc3038bd0550c85b6a3bf", 123)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{}, buffer.UserProvided))

		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "4216455ceebbc3038bd0550c85b6a3bf",
				SizeBytes: 123,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform:           &remoteexecution.Platform{},
		}, key.GetPlatformQueueName())
	})
}
