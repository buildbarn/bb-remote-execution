package initialsizeclass

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAnalyzerFromConfiguration creates a new initial size class
// analyzer based on options provided in a configuration file.
func NewAnalyzerFromConfiguration(configuration *pb.InitialSizeClassAnalyzerConfiguration, previousExecutionStatsStore PreviousExecutionStatsStore) (Analyzer, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No initial size class analyzer configuration provided")
	}

	defaultExecutionTimeout := configuration.DefaultExecutionTimeout
	if err := defaultExecutionTimeout.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid default execution timeout")
	}
	maximumExecutionTimeout := configuration.MaximumExecutionTimeout
	if err := maximumExecutionTimeout.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid maximum execution timeout")
	}
	actionTimeoutExtractor := NewActionTimeoutExtractor(
		defaultExecutionTimeout.AsDuration(),
		maximumExecutionTimeout.AsDuration())

	if fdConfiguration := configuration.FeedbackDriven; fdConfiguration != nil {
		if previousExecutionStatsStore == nil {
			return nil, status.Error(codes.InvalidArgument, "Feedback driven analysis can only be enabled if an Initial Size Class Cache (ISCC) is configured")
		}
		failureCacheDuration := fdConfiguration.FailureCacheDuration
		if err := failureCacheDuration.CheckValid(); err != nil {
			return nil, util.StatusWrap(err, "Invalid failure cache duration")
		}

		strategyCalculator := SmallestSizeClassStrategyCalculator
		if pageRankConfiguration := fdConfiguration.PageRank; pageRankConfiguration != nil {
			minimumExecutionTimeout := pageRankConfiguration.MinimumExecutionTimeout
			if err := minimumExecutionTimeout.CheckValid(); err != nil {
				return nil, util.StatusWrap(err, "Invalid minimum acceptable execution time")
			}
			strategyCalculator = NewPageRankStrategyCalculator(
				minimumExecutionTimeout.AsDuration(),
				pageRankConfiguration.AcceptableExecutionTimeIncreaseExponent,
				pageRankConfiguration.SmallerSizeClassExecutionTimeoutMultiplier,
				pageRankConfiguration.MaximumConvergenceError)
		}

		return NewFeedbackDrivenAnalyzer(
			previousExecutionStatsStore,
			random.NewFastSingleThreadedGenerator(),
			clock.SystemClock,
			actionTimeoutExtractor,
			failureCacheDuration.AsDuration(),
			strategyCalculator,
			int(fdConfiguration.HistorySize)), nil
	}
	return NewFallbackAnalyzer(actionTimeoutExtractor), nil
}
