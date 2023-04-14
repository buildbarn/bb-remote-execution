package platform_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKey(t *testing.T) {
	t.Run("NilPlatform", func(t *testing.T) {
		// Some clients (e.g., Buildstream) don't set the
		// Platform message at all. Treat it as if the message
		// was empty.
		k, err := platform.NewKey(digest.EmptyInstanceName, nil)
		require.NoError(t, err)

		require.Equal(t, digest.EmptyInstanceName, k.GetInstanceNamePrefix())
		require.Equal(t, "{}", k.GetPlatformString())
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "",
			Platform:           &remoteexecution.Platform{},
		}, k.GetPlatformQueueName())
	})

	t.Run("EmptyPlatform", func(t *testing.T) {
		k, err := platform.NewKey(digest.MustNewInstanceName("a/b"), &remoteexecution.Platform{})
		require.NoError(t, err)

		require.Equal(t, digest.MustNewInstanceName("a/b"), k.GetInstanceNamePrefix())
		require.Equal(t, "{}", k.GetPlatformString())
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "a/b",
			Platform:           &remoteexecution.Platform{},
		}, k.GetPlatformQueueName())
	})

	t.Run("InvalidPropertiesOrder", func(t *testing.T) {
		// Platform properties must be provided in sorted order,
		// as the encoding is ambiguous otherwise.
		_, err := platform.NewKey(digest.MustNewInstanceName("a"), &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{
					Name:  "os",
					Value: "linux",
				},
				{
					Name:  "cpu",
					Value: "x86",
				},
			},
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Platform properties are not lexicographically sorted, as property "), err)
	})

	t.Run("MultipleValues", func(t *testing.T) {
		// It is valid to provide multiple values for the same
		// platform property, as long as the values are sorted.
		k, err := platform.NewKey(digest.MustNewInstanceName("a"), &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{
					Name:  "cpu",
					Value: "i386",
				},
				{
					Name:  "cpu",
					Value: "x86_64",
				},
				{
					Name:  "os",
					Value: "linux",
				},
			},
		})
		require.NoError(t, err)

		require.Equal(t, digest.MustNewInstanceName("a"), k.GetInstanceNamePrefix())
		require.Equal(
			t,
			"{\"properties\":[{\"name\":\"cpu\",\"value\":\"i386\"},{\"name\":\"cpu\",\"value\":\"x86_64\"},{\"name\":\"os\",\"value\":\"linux\"}]}",
			k.GetPlatformString())
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "a",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{
						Name:  "cpu",
						Value: "i386",
					},
					{
						Name:  "cpu",
						Value: "x86_64",
					},
					{
						Name:  "os",
						Value: "linux",
					},
				},
			},
		}, k.GetPlatformQueueName())
	})
}
