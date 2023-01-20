package platform_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestStaticKeyExtractor(t *testing.T) {
	keyExtractor := platform.NewStaticKeyExtractor(&remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "arch", Value: "aarch64"},
			{Name: "os", Value: "linux"},
		},
	})
	ctx := context.Background()
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256)

	key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "arch", Value: "x86_64"},
				{Name: "os", Value: "freebsd"},
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
}
