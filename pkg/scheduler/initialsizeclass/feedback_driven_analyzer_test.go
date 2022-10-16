package initialsizeclass_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFeedbackDrivenAnalyzer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	store := mock.NewMockPreviousExecutionStatsStore(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	clock := mock.NewMockClock(ctrl)
	actionTimeoutExtractor := initialsizeclass.NewActionTimeoutExtractor(
		30*time.Minute,
		60*time.Minute)
	strategyCalculator := mock.NewMockStrategyCalculator(ctrl)
	analyzer := initialsizeclass.NewFeedbackDrivenAnalyzer(
		store,
		randomNumberGenerator,
		clock,
		actionTimeoutExtractor,
		/* failureCacheDuration = */ 24*time.Hour,
		strategyCalculator,
		/* historySize = */ 5)

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
	}
	exampleReducedActionDigest := digest.MustNewDigest("hello", "5057a5db1b97ee73b2466f60e781d607", 39)

	t.Run("StorageFailure", func(t *testing.T) {
		// Failures reading existing entries from the Initial
		// Size Class Cache (ISCC) should be propagated.
		store.EXPECT().Get(ctx, exampleReducedActionDigest).
			Return(nil, status.Error(codes.Internal, "Network error"))

		_, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read previous execution stats for reduced action digest \"5057a5db1b97ee73b2466f60e781d607-39-hello\": Network error"), err)
	})

	t.Run("InitialAbandoned", func(t *testing.T) {
		handle := mock.NewMockPreviousExecutionStatsHandle(ctrl)
		store.EXPECT().Get(ctx, exampleReducedActionDigest).Return(handle, nil)

		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)

		// Return an empty stats message. The strategy
		// calculator will most likely just return a uniform
		// distribution. Let's pick the smallest size class.
		var stats iscc.PreviousExecutionStats
		handle.EXPECT().GetMutableProto().Return(&stats).AnyTimes()
		strategyCalculator.EXPECT().GetStrategies(gomock.Not(gomock.Nil()), []uint32{1, 2, 4, 8}, 30*time.Minute).
			Return([]initialsizeclass.Strategy{
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
			})
		randomNumberGenerator.EXPECT().Float64().Return(0.1)

		sizeClassIndex, expectedDuration, timeout, learner := selector.Select([]uint32{1, 2, 4, 8})
		require.Equal(t, 0, sizeClassIndex)
		require.Equal(t, 15*time.Second, expectedDuration)
		require.Equal(t, 15*time.Second, timeout)

		// Action didn't get run after all.
		handle.EXPECT().Release(false)

		learner.Abandoned()
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{},
		}, &stats)
	})

	t.Run("InitialSuccess", func(t *testing.T) {
		handle := mock.NewMockPreviousExecutionStatsHandle(ctrl)
		store.EXPECT().Get(ctx, exampleReducedActionDigest).Return(handle, nil)

		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)

		// Same as before: empty stats message. Now pick the
		// second smallest size class.
		var stats iscc.PreviousExecutionStats
		handle.EXPECT().GetMutableProto().Return(&stats).AnyTimes()
		strategyCalculator.EXPECT().GetStrategies(gomock.Not(gomock.Nil()), []uint32{1, 2, 4, 8}, 30*time.Minute).
			Return([]initialsizeclass.Strategy{
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
				{
					Probability:                0.25,
					ForegroundExecutionTimeout: 15 * time.Second,
				},
			})
		randomNumberGenerator.EXPECT().Float64().Return(0.4)

		sizeClassIndex, expectedDuration, timeout, learner1 := selector.Select([]uint32{1, 2, 4, 8})
		require.Equal(t, 1, sizeClassIndex)
		require.Equal(t, 15*time.Second, expectedDuration)
		require.Equal(t, 15*time.Second, timeout)

		// Report that execution succeeded. This should cause
		// the execution time to be recorded.
		handle.EXPECT().Release(true)

		_, _, _, learner2 := learner1.Succeeded(time.Minute, []uint32{1, 2, 4, 8})
		require.Nil(t, learner2)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				2: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 60}}},
					},
				},
			},
		}, &stats)
	})

	t.Run("SuccessAfterFailure", func(t *testing.T) {
		handle := mock.NewMockPreviousExecutionStatsHandle(ctrl)
		store.EXPECT().Get(ctx, exampleReducedActionDigest).Return(handle, nil)

		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)

		// Let the action run on size class 1.
		stats := iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				8: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 10}}},
					},
				},
			},
		}
		handle.EXPECT().GetMutableProto().Return(&stats).AnyTimes()
		strategyCalculator.EXPECT().GetStrategies(gomock.Not(gomock.Nil()), []uint32{1, 2, 4, 8}, 30*time.Minute).
			Return([]initialsizeclass.Strategy{
				{
					Probability:                0.6,
					ForegroundExecutionTimeout: 40 * time.Second,
				},
				{
					Probability:                0.2,
					ForegroundExecutionTimeout: 30 * time.Second,
				},
				{
					Probability:                0.1,
					ForegroundExecutionTimeout: 20 * time.Second,
				},
			})
		randomNumberGenerator.EXPECT().Float64().Return(0.55)

		sizeClassIndex, expectedDuration1, timeout1, learner1 := selector.Select([]uint32{1, 2, 4, 8})
		require.Equal(t, 0, sizeClassIndex)
		require.Equal(t, 40*time.Second, expectedDuration1)
		require.Equal(t, 40*time.Second, timeout1)

		// Let execution fail on size class 1. Because this is
		// not the largest size class, a new learner for size
		// class 8 is returned.
		expectedDuration2, timeout2, learner2 := learner1.Failed(false)
		require.NotNil(t, learner2)
		require.Equal(t, 10*time.Second, expectedDuration2)
		require.Equal(t, 30*time.Minute, timeout2)

		// Report success on size class 8. This should cause the
		// result of both executions to be stored.
		handle.EXPECT().Release(true)

		_, _, _, learner3 := learner2.Succeeded(12*time.Second, []uint32{1, 2, 4, 8})
		require.Nil(t, learner3)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				1: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
					},
				},
				8: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 10}}},
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 12}}},
					},
				},
			},
		}, &stats)
	})

	t.Run("SkipSmallerAfterFailure", func(t *testing.T) {
		handle := mock.NewMockPreviousExecutionStatsHandle(ctrl)
		store.EXPECT().Get(ctx, exampleReducedActionDigest).Return(handle, nil)

		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)

		// Provide statistics for an action that failed
		// recently. We should always schedule these on the
		// largest size class, so that we don't introduce
		// unnecessary delays.
		stats := iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				8: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 10}}},
					},
				},
			},
			LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620218381},
		}
		handle.EXPECT().GetMutableProto().Return(&stats).AnyTimes()
		clock.EXPECT().Now().Return(time.Unix(1620242374, 0))

		sizeClassIndex, expectedDuration, timeout, learner := selector.Select([]uint32{1, 2, 4, 8})
		require.Equal(t, 3, sizeClassIndex)
		require.Equal(t, 10*time.Second, expectedDuration)
		require.Equal(t, 30*time.Minute, timeout)

		// Abandoning it should not cause any changes to it.
		handle.EXPECT().Release(false)

		learner.Abandoned()
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				8: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 10}}},
					},
				},
			},
			LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620218381},
		}, &stats)
	})

	t.Run("BackgroundRun", func(t *testing.T) {
		handle := mock.NewMockPreviousExecutionStatsHandle(ctrl)
		store.EXPECT().Get(ctx, exampleReducedActionDigest).Return(handle, nil)

		selector, err := analyzer.Analyze(ctx, exampleDigestFunction, exampleAction)
		require.NoError(t, err)

		// Provide statistics for an action that has never been
		// run before. It should be run on the largest size
		// class, but we do want to perform a background run on
		// the smallest size class. If both succeed, we have
		// more freedom when scheduling this action the next
		// time.
		var stats iscc.PreviousExecutionStats
		handle.EXPECT().GetMutableProto().Return(&stats).AnyTimes()
		strategyCalculator.EXPECT().GetStrategies(gomock.Not(gomock.Nil()), []uint32{1, 2, 4, 8}, 30*time.Minute).
			Return([]initialsizeclass.Strategy{
				{
					Probability:     1.0,
					RunInBackground: true,
				},
			})
		randomNumberGenerator.EXPECT().Float64().Return(0.32)

		sizeClassIndex1, expectedDuration1, timeout1, learner1 := selector.Select([]uint32{1, 2, 4, 8})
		require.Equal(t, 3, sizeClassIndex1)
		require.Equal(t, 30*time.Minute, expectedDuration1)
		require.Equal(t, 30*time.Minute, timeout1)

		// Once execution on the largest size class has
		// succeeded, we should obtain a new learner for running
		// it on the smallest size class.
		//
		// Because the execution timeout to be used on the
		// smallest size class depends on that of the largest
		// size class, we should see a request to recompute the
		// execution timeout.
		strategyCalculator.EXPECT().GetBackgroundExecutionTimeout(gomock.Not(gomock.Nil()), []uint32{1, 2, 4, 8}, 0, 30*time.Minute).DoAndReturn(
			func(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, sizeClassIndex int, originalTimeout time.Duration) time.Duration {
				testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
					SizeClasses: map[uint32]*iscc.PerSizeClassStats{
						8: {
							PreviousExecutions: []*iscc.PreviousExecution{
								{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 42}}},
							},
						},
					},
				}, &stats)
				return 80 * time.Second
			})

		sizeClassIndex2, expectedDuration2, timeout2, learner2 := learner1.Succeeded(42*time.Second, []uint32{1, 2, 4, 8})
		require.NotNil(t, learner2)
		require.Equal(t, 0, sizeClassIndex2)
		require.Equal(t, 80*time.Second, expectedDuration2)
		require.Equal(t, 80*time.Second, timeout2)

		// Once execution on the smallest size class completes,
		// both outcomes are stored.
		handle.EXPECT().Release(true)

		_, _, _, learner3 := learner2.Succeeded(72*time.Second, []uint32{1, 2, 4, 8})
		require.Nil(t, learner3)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses: map[uint32]*iscc.PerSizeClassStats{
				1: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 72}}},
					},
				},
				8: {
					PreviousExecutions: []*iscc.PreviousExecution{
						{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 42}}},
					},
				},
			},
		}, &stats)
	})

	// TODO: Are there more test cases we want to cover?
}
