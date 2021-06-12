package initialsizeclass

import (
	"sort"
	"time"
)

// Outcomes of previous executions of an action. For successful
// outcomes, the execution times are stored in ascending order. For
// failures, a count is stored.
type Outcomes struct {
	successes durationsList
	failures  int
}

// NewOutcomes creates a new Outcomes object that contains samples for
// successful and failed executions based on the arguments provided.
// This function takes ownership of the list of durations, sorting it in
// ascending order.
func NewOutcomes(successes []time.Duration, failures int) Outcomes {
	sort.Sort(durationsList(successes))
	return Outcomes{
		successes: successes,
		failures:  failures,
	}
}

// GetMedianExecutionTime computes the median execution time of all of
// the successful outcomes. It may return nil in case no successful
// outcomes have been registered.
func (o Outcomes) GetMedianExecutionTime() *time.Duration {
	if len(o.successes) == 0 {
		return nil
	}
	middle := len(o.successes) / 2
	median := o.successes[middle]
	if len(o.successes)%2 == 0 {
		median = (o.successes[middle-1] + median) / 2
	}
	return &median
}

// IsFaster returns a probability in range (0.0, 1.0) of the current set
// of outcomes being faster than another one. The algorithm for this is
// to compute the average quantile in B for every element in A. This is
// done for two reasons:
//
// - Analysis on mean or median values is not always possible, as a set
//   of outcomes may contain (or consist only of) failures of which the
//   execution time is unknown.
// - When implemented properly, it is an asymmetric relation, in that
//   x.IsFaster(x) == 0.5 and x.IsFaster(y) + y.IsFaster(x) == 1.0 for
//   any sets of outcomes x and y.
//
// This function works by running a 2-way merge algorithm against both
// sets of outcomes, awarding scores between [0, 2*len(B)] based on the
// rank in B for each of the elements in A, meaning a total score of
// 2*len(A)*len(B) may be earned. Inequality between elements always
// yields an even score. Odd scores may need to be given in case of
// identical values.
//
// To ensure that the probability returned by this function doesn't
// become too extreme for small sample counts, we add 1+len(B) to A's
// score, and 1+len(A) to B's score. This also makes sure that empty
// sets don't cause divisions by zero, and that the probability never
// becomes exactly 0.0 or 1.0. The latter is important for PageRank
// computation, as eigenvalue computation wouldn't converge otherwise.
// It also causes smaller sets to get an advantage, which is important
// for ensuring that all size classes are tested sufficiently. This is
// similar in spirit to the "plus four" rule for computing confidence
// intervals.
func (o Outcomes) IsFaster(other Outcomes) float64 {
	successesA, successesB := o.successes, other.successes
	countA, countB := len(successesA)+o.failures, len(successesB)+other.failures
	score := 1 + countB
	remainingA, remainingB := countA, countB
	for len(successesA) > 0 && len(successesB) > 0 {
		if successesA[0] < successesB[0] {
			// The first sample in A is faster than the
			// first sample in B. Award full points.
			score += 2 * remainingB
			successesA = successesA[1:]
			remainingA--
		} else if successesA[0] > successesB[0] {
			// The first sample in A is slower than the
			// first sample in B. Award no points.
			successesB = successesB[1:]
			remainingB--
		} else {
			// First sample in A is identical to the first
			// sample in B. Consume all identical values in
			// A and B and award half points for the entire
			// region.
			equalA, equalB := 1, 1
			current := successesA[0]
			for {
				successesA = successesA[1:]
				if len(successesA) == 0 || successesA[0] != current {
					break
				}
				equalA++
			}
			for {
				successesB = successesB[1:]
				if len(successesB) == 0 || successesB[0] != current {
					break
				}
				equalB++
			}
			score += equalA * (2*remainingB - equalB)
			remainingA -= equalA
			remainingB -= equalB
		}
	}
	// Add score for trailing elements and failures. All failures
	// are effectively treated as having the same execution time,
	// exceeding that of any of the successful outcomes.
	score += 2 * len(successesA) * remainingB
	score += o.failures * other.failures
	return float64(score) / float64(2+countA+countB+2*countA*countB)
}

// durationsList is a list of time.Duration values. It implements
// sort.Interface.
type durationsList []time.Duration

func (l durationsList) Len() int {
	return len(l)
}

func (l durationsList) Less(i, j int) bool {
	return l[i] < l[j]
}

func (l durationsList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
