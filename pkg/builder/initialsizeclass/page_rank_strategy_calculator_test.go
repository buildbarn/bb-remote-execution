package initialsizeclass_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/builder/initialsizeclass"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// If only a single size class is available, there is no need to make
// any choices. We should always run on that size class.
func TestPageRankStrategyCalculatorSingleSizeClass(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	require.Empty(t, strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{}, []uint32{8}, 15*time.Minute))
}

// requireEqualStrategies compares two lists of Strategy objects for
// equality. Probabilities are compared with an error margin of 0.5%.
func requireEqualStrategies(t *testing.T, expected, actual []initialsizeclass.Strategy) {
	require.Len(t, actual, len(expected))
	for i := range actual {
		require.InDelta(t, expected[i].Probability, actual[i].Probability, 0.005, fmt.Sprintf("Index %d", i))
		expectedStrategy := expected[i]
		expectedStrategy.Probability = 0
		actualStrategy := actual[i]
		actualStrategy.Probability = 0
		require.Equal(t, expectedStrategy, actualStrategy, fmt.Sprintf("Index %d", i))
	}
}

// The first time an action is executed, all of the smaller size classes
// should have an equal probability of running the action.
func TestPageRankStrategyCalculatorEmpty(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: {},
		2: {},
		4: {},
		8: {},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				Probability:     1.0,
				RunInBackground: true,
			},
		},
		strategies)
}

// If the action has succeeded once on both the smallest and the largest
// size class, we can assume it's relatively safe to run the action on
// all size classes. We should propose foreground execution on any size
// class. The size classes without any outcomes should have a higher
// probability, so that those also get trained.
func TestPageRankStrategyCalculatorSingleRunSuccess(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 1}}},
			},
		},
		2: {},
		4: {},
		8: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 1}}},
			},
		},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				Probability:                0.19,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
			{
				Probability:                0.33,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
			{
				Probability:                0.33,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
		},
		strategies)
}

// If execution succeeded on the largest and failed on the smallest, the
// smartest thing to do is to schedule a single background run against
// size class 2. The reason being that if we know that that succeeds, we
// don't need to perform any background runs to train size class 4.
func TestPageRankStrategyCalculatorSingleRunFailure(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			},
		},
		2: {},
		4: {},
		8: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 1}}},
			},
		},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				RunInBackground: true,
			},
			{
				Probability:     1.0,
				RunInBackground: true,
			},
		},
		strategies)
}

// When timeoutMultiplier is set to 1.5, an action with a 900s timeout
// should preferably finish within 600s. It may be the case that this
// can't even be achieved on the largest size class, as the action's
// timeout is set to a very tight value.
//
// In this case the largest size class should be the one with the
// highest probability, so that we reduce the need for doing retries.
func TestPageRankStrategyCalculatorCloseToTimeout(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 7, Nanos: 500000000}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
			},
		},
		2: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
			},
		},
		4: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 744, Nanos: 745171748}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 736, Nanos: 585305066}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 786, Nanos: 526637558}}},
				{Outcome: &iscc.PreviousExecution_TimedOut{TimedOut: &durationpb.Duration{Seconds: 900}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 773, Nanos: 860202581}}},
			},
		},
		8: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 652, Nanos: 236376306}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 624, Nanos: 11911117}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 630, Nanos: 320095712}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 627, Nanos: 102638899}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 651, Nanos: 795797310}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 655, Nanos: 97161482}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 649, Nanos: 54963830}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 653, Nanos: 183883239}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 648, Nanos: 783209241}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 666, Nanos: 485370182}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 640, Nanos: 917318827}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 636, Nanos: 910996040}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 669, Nanos: 358977129}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 638, Nanos: 876466482}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 667, Nanos: 615625730}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 639, Nanos: 109428595}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 645, Nanos: 421212352}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 659, Nanos: 724568628}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 645, Nanos: 199012224}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 623, Nanos: 819328226}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 642, Nanos: 84340620}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 633, Nanos: 645871363}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 692, Nanos: 204251786}}},
			},
		},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				Probability:     0.07,
				RunInBackground: true,
			},
			{
				Probability:     0.06,
				RunInBackground: true,
			},
			{
				Probability:     0.07,
				RunInBackground: true,
			},
		},
		strategies)
}

// Size classes for which we don't have any outcomes should always
// receive a high probability. This ensures that we properly test all of
// them.
func TestPageRankStrategyCalculatorUntestedSizeClass(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 0.5, 1.5, 0.001)
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 19941089}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20017118}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21509286}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 31062553}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 32028792}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 56637488}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20011641}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 32338320}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21190311}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 19520433}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 19496810}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 34248944}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 39543182}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21466694}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20287814}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20572146}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20582404}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21701414}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21688507}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20296545}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 19621454}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 41513823}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 22492816}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 20089137}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 36233309}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21063001}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 37055862}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 18909835}}},
			},
		},
		2: {},
		4: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 19648577}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 26058621}}},
			},
		},
		8: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Nanos: 21127338}}},
			},
		},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				Probability:                0.14,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
			{
				Probability:                0.56,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
			{
				Probability:                0.15,
				ForegroundExecutionTimeout: 5 * time.Second,
			},
		},
		strategies)
}

// Test the extreme case, where an action always fails on all size
// classes, except the largest. The resulting probability values should
// be very low.
func TestPageRankStrategyCalculatorExtremelyHighProbability(t *testing.T) {
	strategyCalculator := initialsizeclass.NewPageRankStrategyCalculator(5*time.Second, 1.0, 1.5, 0.001)
	thirtyFailures := iscc.PerSizeClassStats{
		PreviousExecutions: []*iscc.PreviousExecution{
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},

			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},

			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
			{Outcome: &iscc.PreviousExecution_Failed{Failed: &emptypb.Empty{}}},
		},
	}
	strategies := strategyCalculator.GetStrategies(map[uint32]*iscc.PerSizeClassStats{
		1: &thirtyFailures,
		2: &thirtyFailures,
		4: &thirtyFailures,
		8: {
			PreviousExecutions: []*iscc.PreviousExecution{
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 14}}},

				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 15}}},

				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
				{Outcome: &iscc.PreviousExecution_Succeeded{Succeeded: &durationpb.Duration{Seconds: 16}}},
			},
		},
	}, []uint32{1, 2, 4, 8}, 15*time.Minute)
	requireEqualStrategies(
		t,
		[]initialsizeclass.Strategy{
			{
				Probability:     0.02,
				RunInBackground: true,
			},
			{
				Probability:     0.02,
				RunInBackground: true,
			},
			{
				Probability:     0.02,
				RunInBackground: true,
			},
		},
		strategies)
}
