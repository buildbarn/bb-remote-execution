package initialsizeclass_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/stretchr/testify/require"
)

func TestOutcomesIsFasterIdentity(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		// Calling IsFaster() against the same sets should
		// always yield 0.5.
		for _, outcomes := range []initialsizeclass.Outcomes{
			initialsizeclass.NewOutcomes(nil, 0),
			initialsizeclass.NewOutcomes([]time.Duration{
				time.Second,
			}, 0),
			initialsizeclass.NewOutcomes([]time.Duration{
				time.Second,
				time.Second,
			}, 0),
			initialsizeclass.NewOutcomes([]time.Duration{
				7 * time.Second,
				8 * time.Second,
				9 * time.Second,
				10 * time.Second,
				11 * time.Second,
				12 * time.Second,
			}, 14),
		} {
			require.Equal(t, 0.5, outcomes.IsFaster(outcomes))
		}
	})

	t.Run("Asymmetry1", func(t *testing.T) {
		// With one list containing 1 element and the other one
		// being empty, IsFaster() should use a divisor of
		// 2 + 1 + 0 + 1*0 = 3.
		outcomesA := initialsizeclass.NewOutcomes([]time.Duration{
			time.Second,
		}, 0)
		outcomesB := initialsizeclass.NewOutcomes(nil, 0)
		require.Equal(t, float64(1)/3, outcomesA.IsFaster(outcomesB))
		require.Equal(t, float64(2)/3, outcomesB.IsFaster(outcomesA))
	})

	t.Run("Asymmetry2", func(t *testing.T) {
		// With lists of 10 elements, IsFaster() should use a
		// divisor of 2 + 10 + 10 + 2*10*10 = 222.
		outcomesA := initialsizeclass.NewOutcomes([]time.Duration{
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
		}, 0)
		outcomesB := initialsizeclass.NewOutcomes(nil, 10)
		require.Equal(t, float64(211)/222, outcomesA.IsFaster(outcomesB))
		require.Equal(t, float64(11)/222, outcomesB.IsFaster(outcomesA))
	})

	t.Run("Wider", func(t *testing.T) {
		// Samples in both sets center around 10 seconds. It's
		// just that the ones in set A spread a bit wider.
		outcomesA := initialsizeclass.NewOutcomes([]time.Duration{
			6 * time.Second,
			8 * time.Second,
			10 * time.Second,
			10 * time.Second,
			12 * time.Second,
			14 * time.Second,
		}, 1)
		outcomesB := initialsizeclass.NewOutcomes([]time.Duration{
			9 * time.Second,
			9 * time.Second,
			9 * time.Second,
			11 * time.Second,
			11 * time.Second,
			11 * time.Second,
		}, 1)
		require.Equal(t, 0.5, outcomesA.IsFaster(outcomesB))
		require.Equal(t, 0.5, outcomesB.IsFaster(outcomesA))
	})

	t.Run("ZigZagFaster", func(t *testing.T) {
		// The outcomes in sets A and B alternate. Because the
		// outcomes in set A are all slightly smaller, the
		// probability should be in favor of set A.
		outcomesA := initialsizeclass.NewOutcomes([]time.Duration{
			1 * time.Second,
			3 * time.Second,
			5 * time.Second,
			7 * time.Second,
			9 * time.Second,
			11 * time.Second,
		}, 0)
		outcomesB := initialsizeclass.NewOutcomes([]time.Duration{
			2 * time.Second,
			4 * time.Second,
			6 * time.Second,
			8 * time.Second,
			10 * time.Second,
			12 * time.Second,
		}, 0)
		require.Equal(t, float64(49)/86, outcomesA.IsFaster(outcomesB))
		require.Equal(t, float64(37)/86, outcomesB.IsFaster(outcomesA))
	})

	t.Run("ZigZagEqual", func(t *testing.T) {
		// The same sets as before, except that we place another
		// sample at the end of set A. This should bring the
		// probability closer to 0.5. Set B is still preferred,
		// because it has a smaller number of samples.
		outcomesA := initialsizeclass.NewOutcomes([]time.Duration{
			1 * time.Second,
			3 * time.Second,
			5 * time.Second,
			7 * time.Second,
			9 * time.Second,
			11 * time.Second,
			13 * time.Second,
		}, 0)
		outcomesB := initialsizeclass.NewOutcomes([]time.Duration{
			2 * time.Second,
			4 * time.Second,
			6 * time.Second,
			8 * time.Second,
			10 * time.Second,
			12 * time.Second,
		}, 0)
		require.Equal(t, float64(49)/99, outcomesA.IsFaster(outcomesB))
		require.Equal(t, float64(50)/99, outcomesB.IsFaster(outcomesA))
	})
}
