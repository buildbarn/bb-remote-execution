package initialsizeclass

import (
	"context"
	"strconv"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type platformOverridingSizeClassAnalyzer struct {
	platformKey            string
	actionTimeoutExtractor *ActionTimeoutExtractor
	base                   Analyzer
}

func NewPlatformOverridingAnalyzer(actionTimeoutExtractor *ActionTimeoutExtractor, platformKey string, base Analyzer) Analyzer {
	return &platformOverridingSizeClassAnalyzer{
		platformKey:            platformKey,
		actionTimeoutExtractor: actionTimeoutExtractor,
		base:                   base,
	}
}

type sizeClassSelector struct {
	sizeClass string
	timeout   time.Duration
}

func (a *platformOverridingSizeClassAnalyzer) Analyze(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (Selector, error) {
	timeout, err := a.actionTimeoutExtractor.ExtractTimeout(action)
	if err != nil {
		return nil, err
	}

	platform := action.Platform
	for _, key := range platform.Properties {
		if key.Name == a.platformKey {
			return sizeClassSelector{
				sizeClass: key.Value,
				timeout:   timeout,
			}, nil
		}
	}

	if a.base == nil {
		return nil, status.Error(codes.InvalidArgument, "No size class provided and no analyzer configured")
	}

	return a.base.Analyze(ctx, digestFunction, action)
}

type emptyLearner struct{}

func (s sizeClassSelector) Select(sizeClasses []uint32) (int, time.Duration, Learner, error) {
	learner := &emptyLearner{}
	if s.sizeClass == "max" {
		return len(sizeClasses) - 1, s.timeout, learner, nil
	}
	if s.sizeClass == "min" {
		return 0, s.timeout, learner, nil
	}
	class, err := strconv.Atoi(s.sizeClass)
	if err != nil {
		return 0, s.timeout, learner, status.Errorf(codes.InvalidArgument, "Invalid size class '%s'", s.sizeClass)
	}
	for idx, val := range sizeClasses {
		if int(val) == class {
			return idx, s.timeout, learner, nil
		}
	}
	return 0, s.timeout, learner, status.Errorf(codes.InvalidArgument, "Size class '%s' not available", s.sizeClass)
}

func (sizeClassSelector) Abandoned() {}

func (emptyLearner) Succeeded(duration time.Duration, sizeClasses []uint32) (int, time.Duration, Learner) {
	return 0, 0, nil
}

func (emptyLearner) Failed(timedOut bool) (time.Duration, Learner) {
	return 0, nil
}

// Clients have abandoned the action, meaning that execution of
// the action was terminated. Nothing may be learned from this
// action.
func (emptyLearner) Abandoned() {}
