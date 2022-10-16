package initialsizeclass

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Analyzer of REv2 Actions, determining which worker size class is the
// most suitable for running it, and what timeout to use.
type Analyzer interface {
	// Analyze an REv2 Action. This operation should be called
	// without holding any locks, as it may block.
	//
	// All methods called against the Selector that is returned and
	// any Learners yielded from it should be called while holding a
	// global lock.
	Analyze(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (Selector, error)
}

// Selector of a size class for an analyzed action.
type Selector interface {
	// Given a list of size classes that are currently present,
	// select a size class on which the action needs to be
	// scheduled.
	//
	// The Selector can override the timeout of the action. When
	// attempting to run an action on a smaller size class, it may
	// be desirable to put a timeout in place that is based on
	// previous execution times observed on larger size classes.
	// This ensures that the system remains responsive, even in the
	// case of mispredictions.
	//
	// A Learner is returned that the scheduler must use to
	// communicate the outcome of the execution, so that future
	// executions have a lower probability of making mispredictions.
	Select(sizeClasses []uint32) (sizeClass int, expectedDuration, timeout time.Duration, learner Learner)

	// Clients have abandoned the action, meaning that no size class
	// selection decision needs to be made. This may, for example,
	// happen if the action is deduplicated against one that is
	// already running.
	Abandoned()
}

// Learner for size class selection. The information provided by the
// scheduler to this object may allow the Analyzer and Selector to make
// more accurate predictions in the future.
type Learner interface {
	// The action completed successfully. The execution time is
	// provided.
	//
	// If this method returns a nil Learner, the scheduler can
	// finalize the operation entirely. If this method returns a new
	// Learner, the scheduler is requested to run the action another
	// time in the background, just for learning purposes. It is
	// valid for the scheduler to already communicate completion to
	// the client. The scheduler may limit the amount of work it's
	// willing to run in the background.
	Succeeded(duration time.Duration, sizeClasses []uint32) (sizeClass int, expectedDuration, timeout time.Duration, learner Learner)

	// The action completed with a failure.
	//
	// If this method returns a nil Learner, the execution failure
	// is definitive and should be propagated to the client. If this
	// method returns a new Learner, execution must be retried on
	// the largest size class, using the timeout that is returned.
	Failed(timedOut bool) (expectedDuration, timeout time.Duration, learner Learner)

	// Clients have abandoned the action, meaning that execution of
	// the action was terminated. Nothing may be learned from this
	// action.
	Abandoned()
}
