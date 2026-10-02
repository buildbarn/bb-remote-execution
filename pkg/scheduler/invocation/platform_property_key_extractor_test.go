package invocation_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
)

func TestPlatformPropertyInvocationKeyExtractor(t *testing.T) {
	ctx := context.Background()
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256)
	keyExtractor := invocation.NewPlatformPropertyKeyExtractor("persistentWorkerKey")
	requestMetadata := &remoteexecution.RequestMetadata{
		ToolInvocationId: "9c9e7705-d757-4e57-b0df-58bc69c1cb51",
	}

	t.Run("PropertyPresent", func(t *testing.T) {
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
				},
			},
		}, requestMetadata)
		require.NoError(t, err)
		id, err := anypb.New(&buildqueuestate.PlatformProperty{
			Name:  "persistentWorkerKey",
			Value: "c1f9a75f5d2a3d4a",
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, id, key.GetID())
	})

	t.Run("PropertyAbsent", func(t *testing.T) {
		// Actions that are not executed by a persistent worker
		// all need to be grouped together.
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
				},
			},
		}, requestMetadata)
		require.NoError(t, err)
		id, err := anypb.New(&buildqueuestate.PlatformProperty{
			Name: "persistentWorkerKey",
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, id, key.GetID())

		noPlatformKey, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{}, requestMetadata)
		require.NoError(t, err)
		require.Equal(t, key, noPlatformKey)
	})

	t.Run("DistinctToolsYieldDistinctKeys", func(t *testing.T) {
		keyA, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "aaaaaaaaaaaaaaaa"},
				},
			},
		}, requestMetadata)
		require.NoError(t, err)
		keyB, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "bbbbbbbbbbbbbbbb"},
				},
			},
		}, requestMetadata)
		require.NoError(t, err)
		require.NotEqual(t, keyA, keyB)
	})
}
