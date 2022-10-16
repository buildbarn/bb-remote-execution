package initialsizeclass

import (
	"math"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pageRankStrategyCalculatorMetrics sync.Once

	pageRankStrategyCalculatorConvergenceIterations = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "page_rank_strategy_calculator_convergence_iterations",
			Help:      "Number of iterations matrix multiplication was performed until convergence.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 11),
		})
)

type pageRankStrategyCalculator struct {
	minimumExecutionTimeout                 time.Duration
	acceptableExecutionTimeIncreaseExponent float64
	timeoutMultiplier                       float64
	maximumConvergenceError                 float64
}

// NewPageRankStrategyCalculator creates a StrategyCalculator that uses
// outcomes of previous executions to determine probabilities for
// running actions on a given set of size classes.
//
// The algorithm that it uses to compute probabilities is similar to
// PageRank, in that it constructs a stochastic matrix of which the
// resulting eigenvector contains the probabilities.
func NewPageRankStrategyCalculator(minimumExecutionTimeout time.Duration, acceptableExecutionTimeIncreaseExponent, timeoutMultiplier, maximumConvergenceError float64) StrategyCalculator {
	pageRankStrategyCalculatorMetrics.Do(func() {
		prometheus.MustRegister(pageRankStrategyCalculatorConvergenceIterations)
	})

	return &pageRankStrategyCalculator{
		minimumExecutionTimeout:                 minimumExecutionTimeout,
		acceptableExecutionTimeIncreaseExponent: acceptableExecutionTimeIncreaseExponent,
		timeoutMultiplier:                       timeoutMultiplier,
		maximumConvergenceError:                 maximumConvergenceError,
	}
}

// getOutcomesFromPreviousExecutions returns an Outcomes object that
// stores all execution times observed on a given size class. The
// results are not normalized with respect to other size classes.
func getOutcomesFromPreviousExecutions(previousExecutionsOnLargest []*iscc.PreviousExecution) Outcomes {
	executionTimesOnLargest := make([]time.Duration, 0, len(previousExecutionsOnLargest))
	for _, previousExecution := range previousExecutionsOnLargest {
		if outcome, ok := previousExecution.Outcome.(*iscc.PreviousExecution_Succeeded); ok {
			executionTimesOnLargest = append(executionTimesOnLargest, outcome.Succeeded.AsDuration())
		}
	}
	return NewOutcomes(executionTimesOnLargest, 0)
}

// smallerSizeClassExecutionParameters contains the acceptable execution
// time and the desirable execution timeout to use when executing an
// action on a smaller size class.
type smallerSizeClassExecutionParameters struct {
	acceptableExecutionTimeIncreaseFactor float64
	maximumAcceptableExecutionTime        time.Duration
	executionTimeout                      time.Duration
}

// getSmallerSizeClassExecutionParameters computes the acceptable
// execution time and desirable execution timeout for a given size
// class.
func (sc *pageRankStrategyCalculator) getSmallerSizeClassExecutionParameters(smallerSizeClass, largestSizeClass uint32, medianExecutionTimeOnLargest, originalTimeout time.Duration) (p smallerSizeClassExecutionParameters) {
	p.acceptableExecutionTimeIncreaseFactor = math.Pow(float64(largestSizeClass)/float64(smallerSizeClass), sc.acceptableExecutionTimeIncreaseExponent)
	p.maximumAcceptableExecutionTime = time.Duration(float64(medianExecutionTimeOnLargest) * p.acceptableExecutionTimeIncreaseFactor)
	p.executionTimeout = time.Duration(float64(p.maximumAcceptableExecutionTime) * sc.timeoutMultiplier)
	if p.executionTimeout < sc.minimumExecutionTimeout {
		p.executionTimeout = sc.minimumExecutionTimeout
	}
	if p.executionTimeout > originalTimeout {
		p.executionTimeout = originalTimeout
	}
	if ceiling := time.Duration(float64(p.executionTimeout) / sc.timeoutMultiplier); p.maximumAcceptableExecutionTime > ceiling {
		// Make sure the maximum acceptable execution
		// time is not too close to the execution timeout.
		p.maximumAcceptableExecutionTime = ceiling
	}
	return
}

func (sc *pageRankStrategyCalculator) GetStrategies(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, originalTimeout time.Duration) []Strategy {
	// No need to compute strategies in case there is only one size
	// class available.
	if len(sizeClasses) <= 1 {
		return nil
	}

	// Extract statistics for each of the size classes from the
	// existing stats message. Create a new map entry for each of
	// the size classes not seen before.
	perSizeClassStatsList := make([]*iscc.PerSizeClassStats, 0, len(perSizeClassStatsMap))
	for _, sizeClass := range sizeClasses {
		perSizeClassStats, ok := perSizeClassStatsMap[sizeClass]
		if !ok {
			perSizeClassStats = &iscc.PerSizeClassStats{}
			perSizeClassStatsMap[sizeClass] = perSizeClassStats
		}
		perSizeClassStatsList = append(perSizeClassStatsList, perSizeClassStats)
	}

	// Extract previous execution times on the largest size class.
	// Compute the median, which we'll use as the baseline for
	// determining what the execution timeout should be on smaller
	// size classes.
	n := len(sizeClasses)
	outcomesOnLargest := getOutcomesFromPreviousExecutions(perSizeClassStatsList[n-1].PreviousExecutions)
	medianExecutionTimeOnLargest := outcomesOnLargest.GetMedianExecutionTime()
	if medianExecutionTimeOnLargest == nil {
		// This action never succeeded on the largest size
		// class. Force a run on both the largest and smallest
		// size class. That way we both obtain a median
		// execution time and learn whether the action can run
		// on any size class.
		return []Strategy{
			{
				Probability:     1.0,
				RunInBackground: true,
			},
		}
	}

	// Extract previous execution times on all other size classes.
	largestSizeClass := sizeClasses[n-1]
	outcomesList := make([]Outcomes, 0, n)
	strategies := make([]Strategy, 0, n)
	runInBackground := true
	for i, sizeClass := range sizeClasses[:n-1] {
		// Extract previous execution times on the smaller size
		// class, normalized to the equivalent on the largest
		// size class. Treat execution times that are not
		// acceptable as failures, so that the probability of
		// picking this size class is reduced.
		p := sc.getSmallerSizeClassExecutionParameters(sizeClass, largestSizeClass, *medianExecutionTimeOnLargest, originalTimeout)
		previousExecutionsOnSmaller := perSizeClassStatsList[i].PreviousExecutions
		normalizedExecutionTimes := make(durationsList, 0, len(previousExecutionsOnSmaller))
		failuresOrTimeouts := 0
		for _, previousExecution := range previousExecutionsOnSmaller {
			switch outcome := previousExecution.Outcome.(type) {
			case *iscc.PreviousExecution_Failed:
				failuresOrTimeouts++
			case *iscc.PreviousExecution_TimedOut:
				if duration := outcome.TimedOut.AsDuration(); duration >= p.maximumAcceptableExecutionTime {
					failuresOrTimeouts++
				}
			case *iscc.PreviousExecution_Succeeded:
				if duration := outcome.Succeeded.AsDuration(); duration < p.maximumAcceptableExecutionTime {
					normalizedExecutionTimes = append(normalizedExecutionTimes, time.Duration(float64(duration)/p.acceptableExecutionTimeIncreaseFactor))
				} else {
					failuresOrTimeouts++
				}
			}
		}
		outcomes := NewOutcomes(normalizedExecutionTimes, failuresOrTimeouts)
		outcomesList = append(outcomesList, outcomes)

		if failuresOrTimeouts == 0 && len(normalizedExecutionTimes) == 0 {
			if runInBackground {
				// We have no outcomes for this size
				// class, but we do know that it fails
				// on the size class before it.
				//
				// Do a forced background run on this
				// specific size class. If it succeeds,
				// we know exactly where the tipping
				// point is between success and failure.
				// This reduces the need for background
				// execution (and thus execution on the
				// largest size class) later on.
				return append(strategies, Strategy{
					Probability:     1.0,
					RunInBackground: true,
				})
			}
		} else {
			// We have outcomes for this size class. If
			// there is a more than 50% of failure, run this
			// action in the background. This ensures that
			// the critical path duration of builds remains
			// low. If no outcomes are available, we simply
			// inherit the behaviour from smaller size
			// classes.
			runInBackground = failuresOrTimeouts > len(normalizedExecutionTimes)
		}
		if runInBackground {
			strategies = append(strategies, Strategy{
				RunInBackground: runInBackground,
			})
		} else {
			strategies = append(strategies, Strategy{
				ForegroundExecutionTimeout: p.executionTimeout,
			})
		}
	}
	outcomesList = append(outcomesList, outcomesOnLargest)
	strategies = append(strategies, Strategy{})

	// Create square matrix M with the size corresponding to
	// the number of size classes. In each cell we store the
	// probability of one size class being faster than the
	// other. These values are normalized, so that it is a
	// left stochastic matrix.
	//
	// Because Outcomes.IsFaster() is symmetric, we only
	// need to call it once for every pair (i.e.,
	// (n-1)*(n-2) times).
	mFields := make([]float64, n*n)
	m := make([][]float64, 0, n)
	for i := 0; i < n; i++ {
		mFields[i] = 1.0
		m = append(m, mFields[:n])
		mFields = mFields[n:]
	}
	for i := 1; i < n; i++ {
		for j := 0; j < i; j++ {
			probability := outcomesList[i].IsFaster(outcomesList[j])
			p1 := probability / float64(n-1)
			m[j][i] = p1
			m[j][j] -= p1
			p2 := (1.0 - probability) / float64(n-1)
			m[i][j] = p2
			m[i][i] -= p2
		}
	}

	// Restore previously computed probabilities from the
	// ISCC entry. Using these as a starting point has the
	// advantage that we need fewer rounds of the matrix
	// multiplication below.
	//
	// Only restore probabilities that are in range. Also
	// infer the first entry from the others, so that
	// rounding errors don't accumulate over time.
	var probabilitiesSum float64
	for i := 1; i < n; i++ {
		probability := 0.5
		if restoredProbability := perSizeClassStatsList[i].InitialPageRankProbability; restoredProbability > 0 && restoredProbability < 1 {
			probability = restoredProbability
		}
		strategies[i].Probability = probability
		probabilitiesSum += probability
	}
	strategies[0].Probability = 1.0 - probabilitiesSum

	// Perform power iteration to compute the eigenvector of
	// M, continuing until the rate of convergence drops
	// below a certain minimum.
	newProbabilities := make([]float64, n)
	convergenceIterations := 0
	for {
		for i := 0; i < n; i++ {
			newProbabilities[i] = 0
		}
		for i, column := range m {
			for j, v := range column {
				newProbabilities[j] += strategies[i].Probability * v
			}
		}
		convergenceIterations++

		convergenceError := 0.0
		for i := 0; i < n; i++ {
			convergenceError += math.Abs(strategies[i].Probability - newProbabilities[i])
			strategies[i].Probability = newProbabilities[i]
		}
		if convergenceError < sc.maximumConvergenceError {
			break
		}
	}
	pageRankStrategyCalculatorConvergenceIterations.Observe(float64(convergenceIterations))

	// Save the probabilities that have been computed.
	for _, perSizeClassStats := range perSizeClassStatsMap {
		perSizeClassStats.InitialPageRankProbability = 0
	}
	for i, perSizeClassStats := range perSizeClassStatsList {
		perSizeClassStats.InitialPageRankProbability = strategies[i].Probability
	}
	return strategies[:n-1]
}

func (sc *pageRankStrategyCalculator) GetBackgroundExecutionTimeout(perSizeClassStatsMap map[uint32]*iscc.PerSizeClassStats, sizeClasses []uint32, sizeClassIndex int, originalTimeout time.Duration) time.Duration {
	// Trimmed down version of the algorithm above that is only
	// capable of returning the execution timeout for a given size
	// class. This is used to obtain the most up-to-date value of
	// the execution timeout in case of background runs.
	largestSizeClass := sizeClasses[len(sizeClasses)-1]
	return sc.getSmallerSizeClassExecutionParameters(
		sizeClasses[sizeClassIndex],
		largestSizeClass,
		*getOutcomesFromPreviousExecutions(
			perSizeClassStatsMap[largestSizeClass].PreviousExecutions,
		).GetMedianExecutionTime(),
		originalTimeout,
	).executionTimeout
}
