package initialsizeclass

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
)

// PreviousExecutionStatsStore is a store for PreviousExecutionStats
// messages, allowing them both to be read and written. Because multiple
// operations in the scheduler may be associated with a single
// PreviousExecutionStats message, this interface permits concurrent
// access to the same PreviousExecutionStats message.
//
// The Get() function may be called in parallel, yielding a
// PreviousExecutionStatsHandle. Because these handles are shared, all
// methods on all handles obtained from a single store must be called
// while holding a global lock. The PreviousExecutionStats message
// embedded in the handle gets invalidated after locks are dropped.
type PreviousExecutionStatsStore interface {
	Get(ctx context.Context, reducedActionDigest digest.Digest) (PreviousExecutionStatsHandle, error)
}

// PreviousExecutionStatsHandle is a handle that is returned by
// PreviousExecutionStatsStore. It contains a PreviousExecutionStats
// message that contains timing information of previous executions of
// similar actions.
type PreviousExecutionStatsHandle interface {
	GetPreviousExecutionStats() *iscc.PreviousExecutionStats
	Release(isDirty bool)
}
