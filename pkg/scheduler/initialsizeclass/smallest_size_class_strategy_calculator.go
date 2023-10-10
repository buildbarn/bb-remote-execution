package initialsizeclass

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
)

type smallestSizeClassStrategyCalculator struct{}

func (sc smallestSizeClassStrategyCalculator) GetStrategies(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, originalTimeout time.Duration) []Strategy {
	if len(sizeClasses) <= 1 {
		return nil
	}
	return []Strategy{
		{
			Probability:                1.0,
			ForegroundExecutionTimeout: originalTimeout,
		},
	}
}

func (sc smallestSizeClassStrategyCalculator) GetBackgroundExecutionTimeout(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, sizeClassIndex int, originalTimeout time.Duration) time.Duration {
	panic("Background execution should not be performed")
}

// SmallestSizeClassStrategyCalculator implements a StrategyCalculator
// that always prefers running actions on the smallest size class.
//
// This StrategyCalculator behaves similar to FallbackAnalyzer, with the
// main difference that it still causes execution times and outcomes to
// be tracked in the Initial Size Class Cache (ISCC).
var SmallestSizeClassStrategyCalculator StrategyCalculator = smallestSizeClassStrategyCalculator{}
