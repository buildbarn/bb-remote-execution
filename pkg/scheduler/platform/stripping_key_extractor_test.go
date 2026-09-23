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

func TestStrippingKeyExtractor(t *testing.T) {
	keyExtractor := platform.NewStrippingKeyExtractor(
		platform.ActionKeyExtractor,
		[]string{"persistentWorkerKey", "persistentWorkerProtocol"},
	)
	ctx := context.Background()
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256)

	t.Run("NoPlatform", func(t *testing.T) {
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform:           &remoteexecution.Platform{},
		}, key.GetPlatformQueueName())
	})

	t.Run("NothingToStrip", func(t *testing.T) {
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "container-image", Value: "docker://example.com/image"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "container-image", Value: "docker://example.com/image"},
				},
			},
		}, key.GetPlatformQueueName())
	})

	t.Run("StripSingleProperty", func(t *testing.T) {
		// Bazel adds a 'persistentWorkerKey' platform property
		// that differs for every tool. If it were part of the
		// platform key, no worker would ever match.
		action := &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "container-image", Value: "docker://example.com/image"},
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
				},
			},
		}
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, action)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "container-image", Value: "docker://example.com/image"},
				},
			},
		}, key.GetPlatformQueueName())

		// The action provided by the caller must remain
		// unmodified, as the worker still needs to be able to
		// observe the properties that were stripped.
		testutil.RequireEqualProto(t, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "container-image", Value: "docker://example.com/image"},
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
				},
			},
		}, action)
	})

	t.Run("StripMultipleProperties", func(t *testing.T) {
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
					{Name: "persistentWorkerProtocol", Value: "json"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: "hello",
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
				},
			},
		}, key.GetPlatformQueueName())
	})

	t.Run("StripAllProperties", func(t *testing.T) {
		// Actions whose platform properties are removed
		// entirely must yield the same key as actions that
		// declare no platform properties at all, so that they
		// are routed to the same workers.
		key, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
				},
			},
		})
		require.NoError(t, err)

		emptyKey, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{})
		require.NoError(t, err)
		require.Equal(t, emptyKey, key)
	})

	t.Run("DifferentKeysYieldIdenticalPlatformKeys", func(t *testing.T) {
		keyA, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "persistentWorkerKey", Value: "aaaaaaaaaaaaaaaa"},
				},
			},
		})
		require.NoError(t, err)
		keyB, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "persistentWorkerKey", Value: "bbbbbbbbbbbbbbbb"},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, keyA, keyB)
	})

	t.Run("UnsortedProperties", func(t *testing.T) {
		// Errors generated by the underlying key extractor
		// should be propagated. Note that removing properties
		// can never cause a sorted list to become unsorted.
		_, err := keyExtractor.ExtractKey(ctx, digestFunction, &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "os", Value: "linux"},
					{Name: "arch", Value: "x86_64"},
					{Name: "persistentWorkerKey", Value: "c1f9a75f5d2a3d4a"},
				},
			},
		})
		require.Error(t, err)
	})
}
