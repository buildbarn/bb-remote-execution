package initialsizeclass_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPlatformOverridingAnalyzer(t *testing.T) {
	ctx := context.Background()
	analyzer := initialsizeclass.NewPlatformOverridingAnalyzer(
		initialsizeclass.NewActionTimeoutExtractor(
			30*time.Minute,
			60*time.Minute),
		"size-class",
		nil)

	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "size-class", Value: "0"},
		},
	}
	property := platform.Properties[0]

	exampleDigestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)
	exampleAction := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "bf4d2b56ee892c3fc862cdb863a6c2f4",
			SizeBytes: 720,
		},
		InputRootDigest: &remoteexecution.Digest{
			Hash:      "9a0be105e682022830da33578b909521",
			SizeBytes: 951,
		},
		Timeout:  &durationpb.Duration{Seconds: 300},
		Platform: platform,
	}

	t.Run("SizeClassMax", func(t *testing.T) {
		property.Value = "max"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)
		sizeClassIndex, timeout1, learner1, err := selector.Select([]uint32{1, 2, 4, 8})
		require.NoError(t, err)
		require.Equal(t, 3, sizeClassIndex)
		require.Equal(t, 300*time.Second, timeout1)
		require.NotNil(t, learner1)
	})

	t.Run("SizeClassMin", func(t *testing.T) {
		property.Value = "min"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)
		sizeClassIndex, timeout1, learner1, err := selector.Select([]uint32{1, 2, 4, 8})
		require.NoError(t, err)
		require.Equal(t, 0, sizeClassIndex)
		require.Equal(t, 300*time.Second, timeout1)
		require.NotNil(t, learner1)
	})

	t.Run("SizeClassExact", func(t *testing.T) {
		property.Value = "4"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)
		sizeClassIndex, timeout1, learner1, err := selector.Select([]uint32{1, 2, 4, 8})
		require.NoError(t, err)
		require.Equal(t, 2, sizeClassIndex)
		require.Equal(t, 300*time.Second, timeout1)
		require.NotNil(t, learner1)
	})

	t.Run("SizeClassMissing", func(t *testing.T) {
		property.Value = "5"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)
		_, _, _, err = selector.Select([]uint32{1, 2, 4, 8})
		require.Error(t, err)
	})

	t.Run("NoPropertyNoFallback", func(t *testing.T) {
		property.Name = "some-property"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		_, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.Error(t, err)
	})
}

func TestPlatformOverridingAnalyzerWithFallback(t *testing.T) {
	ctx := context.Background()
	extractor := initialsizeclass.NewActionTimeoutExtractor(
		30*time.Minute,
		60*time.Minute)
	analyzer := initialsizeclass.NewPlatformOverridingAnalyzer(
		extractor,
		"size-class",
		initialsizeclass.NewFallbackAnalyzer(extractor))

	platform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "something", Value: "else"},
		},
	}
	property := platform.Properties[0]

	exampleDigestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)
	exampleAction := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "bf4d2b56ee892c3fc862cdb863a6c2f4",
			SizeBytes: 720,
		},
		InputRootDigest: &remoteexecution.Digest{
			Hash:      "9a0be105e682022830da33578b909521",
			SizeBytes: 951,
		},
		Timeout:  &durationpb.Duration{Seconds: 300},
		Platform: platform,
	}

	t.Run("NoPropertyWithFallback", func(t *testing.T) {
		property.Name = "some-property"
		// Action that is analyzed, but that is immediately
		// abandoned. This should have no effect.
		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)
		sizeClassIndex, timeout1, _, err := selector.Select([]uint32{1, 2, 4, 8})
		require.NoError(t, err)
		require.Equal(t, 0, sizeClassIndex)
		require.Equal(t, 300*time.Second, timeout1)
	})
}
