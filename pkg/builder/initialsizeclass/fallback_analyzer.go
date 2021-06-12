package initialsizeclass

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type fallbackAnalyzer struct {
	actionTimeoutExtractor *ActionTimeoutExtractor
}

// NewFallbackAnalyzer creates a simple Analyzer that runs all actions
// on the smallest size class. Upon failure, the action is retried on
// the largest size class.
//
// This Analyzer is ideal for setups that only use a single size class,
// or if the number of actions that does not succeed on the smallest
// size cache is very low. Any more complex setup should use
// FeedbackDrivenAnalyzer.
func NewFallbackAnalyzer(actionTimeoutExtractor *ActionTimeoutExtractor) Analyzer {
	return &fallbackAnalyzer{
		actionTimeoutExtractor: actionTimeoutExtractor,
	}
}

func (a *fallbackAnalyzer) Analyze(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (Selector, error) {
	timeout, err := a.actionTimeoutExtractor.ExtractTimeout(action)
	if err != nil {
		return nil, err
	}
	return fallbackSelector{
		timeout: timeout,
	}, nil
}

type fallbackSelector struct {
	timeout time.Duration
}

func (s fallbackSelector) Select(sizeClasses []uint32) (int, time.Duration, Learner) {
	if len(sizeClasses) > 1 {
		// Multiple size classes available. Run all actions on
		// the smallest size class, falling back to the largest.
		return 0, s.timeout, smallerFallbackLearner{
			timeout: s.timeout,
		}
	}
	return 0, s.timeout, largestFallbackLearner{}
}

func (fallbackSelector) Abandoned() {}

type fallbackLearner struct{}

func (fallbackLearner) Succeeded(duration time.Duration, sizeClasses []uint32) (int, time.Duration, Learner) {
	// There is no learning that needs to be performed in the
	// background.
	return 0, 0, nil
}

func (fallbackLearner) Abandoned() {}

type smallerFallbackLearner struct {
	fallbackLearner
	timeout time.Duration
}

func (l smallerFallbackLearner) Failed(timedOut bool) (time.Duration, Learner) {
	// Action failed on a smaller size class. Retry on the largest
	// size class.
	return l.timeout, largestFallbackLearner{}
}

type largestFallbackLearner struct {
	fallbackLearner
}

func (largestFallbackLearner) Failed(timedOut bool) (time.Duration, Learner) {
	// Action failed on the largest size class.
	return 0, nil
}
