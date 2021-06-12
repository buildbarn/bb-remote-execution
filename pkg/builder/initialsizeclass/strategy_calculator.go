package initialsizeclass

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
)

// Strategy for running an action on a size class that is not the
// largest size class.
type Strategy struct {
	// Probability between [0.0, 1.0] at which this strategy should
	// be chosen. The sum of all probabilities returned by
	// GetStrategies() should at most be 1.0. If the sum of all
	// probabilities is less than 1.0, the remainder should be the
	// probability of running the action on the largest size class.
	Probability float64
	// Whether the action has a high probability of failing. In that
	// case it is preferable to run the action on the largest size
	// class immediately, only running it on the smaller size class
	// in the background afterwards.
	RunInBackground bool
	// The execution timeout to use when running this action in the
	// foreground on this size class. For the largest size class,
	// the original timeout value should be used.
	//
	// To obtain the execution timeout when running this action in
	// the background, a separate call to
	// GetBackgroundExecutionTimeout() needs to be made. This
	// ensures that the latest obtained execution time of the
	// foreground execution on the largest size class is taken into
	// account when computing the timeout for the smaller size
	// class.
	ForegroundExecutionTimeout time.Duration
}

// StrategyCalculator is responsible for computing the probabilities for
// choosing to run an action on size classes. Given a list of n size
// classes, this function will return a list of n-1 strategies for
// running the action on the smaller size classes.
//
// No strategy for the largest size class is returned, as both is
// probability and options can be inferred.
type StrategyCalculator interface {
	GetStrategies(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, originalTimeout time.Duration) []Strategy
	GetBackgroundExecutionTimeout(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, sizeClassIndex int, originalTimeout time.Duration) time.Duration
}
