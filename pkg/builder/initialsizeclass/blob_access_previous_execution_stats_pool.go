package initialsizeclass

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	blobAccessPreviousExecutionStatsHandleMetrics sync.Once

	blobAccessPreviousExecutionStatsHandlesCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "blob_access_previous_executions_stats_handles_created_total",
			Help:      "Number of previous execution stats handles that were created during Get()",
		})
	blobAccessPreviousExecutionStatsHandlesDestroyed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "blob_access_previous_executions_stats_handles_destroyed_total",
			Help:      "Number of previous execution stats handles that were destroyed",
		})
	blobAccessPreviousExecutionStatsHandlesDequeued = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "blob_access_previous_executions_stats_handles_dequeued_total",
			Help:      "Number of previous execution stats handles that were dequeued for writing during Get()",
		})

	blobAccessPreviousExecutionStatsHandlesQueued = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "blob_access_previous_executions_stats_handles_queued_total",
			Help:      "Number of previous execution stats handles that were queued for writing",
		})
)

type blobAccessPreviousExecutionStatsStore struct {
	initialSizeClassCache   blobstore.BlobAccess
	maximumMessageSizeBytes int

	lock           sync.Mutex
	handles        map[digest.Digest]*blobAccessPreviousExecutionStatsHandle
	handlesToWrite []*blobAccessPreviousExecutionStatsHandle
}

// NewBlobAccessPreviousExecutionStatsStore creates an instance of
// PreviousExecutionStatsStore that is backed by the Initial Size Class
// Cache (ISCC).
//
// What makes this interface harder to implement is that releasing
// PreviousExecutionStatsHandle is performed while holding locks. We
// can't block, nor can we propagate errors or perform retries. To solve
// this, this implementation keeps track of a list of all handles that
// need to be written. Every time a handle is created, we write a couple
// of released handles back to storage. This ensures that the number of
// handles remains proportional to actual use.
func NewBlobAccessPreviousExecutionStatsStore(initialSizeClassCache blobstore.BlobAccess, maximumMessageSizeBytes int) PreviousExecutionStatsStore {
	blobAccessPreviousExecutionStatsHandleMetrics.Do(func() {
		prometheus.MustRegister(blobAccessPreviousExecutionStatsHandlesCreated)
		prometheus.MustRegister(blobAccessPreviousExecutionStatsHandlesDestroyed)
		prometheus.MustRegister(blobAccessPreviousExecutionStatsHandlesDequeued)
		prometheus.MustRegister(blobAccessPreviousExecutionStatsHandlesQueued)
	})

	return &blobAccessPreviousExecutionStatsStore{
		initialSizeClassCache:   initialSizeClassCache,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		handles:                 map[digest.Digest]*blobAccessPreviousExecutionStatsHandle{},
	}
}

func (ss *blobAccessPreviousExecutionStatsStore) Get(ctx context.Context, reducedActionDigest digest.Digest) (PreviousExecutionStatsHandle, error) {
	type handleToWrite struct {
		handle         *blobAccessPreviousExecutionStatsHandle
		stats          proto.Message
		writingVersion int
	}
	const writesPerRead = 3
	handlesToWrite := make([]handleToWrite, 0, writesPerRead)

	// See if a handle for the current reduced action digest already
	// exists that we can use. Remove it from the write queue to
	// prevent unnecessary writes to the ISCC.
	ss.lock.Lock()
	handleToReturn, hasExistingHandle := ss.handles[reducedActionDigest]
	if hasExistingHandle {
		handleToReturn.increaseUseCount()
	}

	// Extract a couple of handles from previous actions that we can
	// write to storage at this point. It is safe to access
	// handle.stats here, as handle.useCount is guaranteed to be
	// zero for handles that are queued for writing.
	for i := 0; i < writesPerRead && len(ss.handlesToWrite) > 0; i++ {
		newLength := len(ss.handlesToWrite) - 1
		handle := ss.handlesToWrite[newLength]
		ss.handlesToWrite[newLength] = nil
		ss.handlesToWrite = ss.handlesToWrite[:newLength]
		if handle.handlesToWriteIndex != newLength {
			panic("Handle has bad write index")
		}
		handle.handlesToWriteIndex = -1
		handlesToWrite = append(handlesToWrite, handleToWrite{
			handle:         handle,
			stats:          proto.Clone(&handle.stats),
			writingVersion: handle.currentVersion,
		})
	}
	ss.lock.Unlock()
	blobAccessPreviousExecutionStatsHandlesDequeued.Add(float64(len(handlesToWrite)))

	// If no handle exists, create a new handle containing
	// existing statistics for the action.
	group, ctxWithCancel := errgroup.WithContext(ctx)
	if !hasExistingHandle {
		handleToReturn = &blobAccessPreviousExecutionStatsHandle{
			store:               ss,
			digest:              reducedActionDigest,
			useCount:            1,
			handlesToWriteIndex: -1,
		}
		group.Go(func() error {
			if m, err := ss.initialSizeClassCache.Get(ctxWithCancel, reducedActionDigest).ToProto(&iscc.PreviousExecutionStats{}, ss.maximumMessageSizeBytes); err == nil {
				proto.Merge(&handleToReturn.stats, m)
			} else if status.Code(err) != codes.NotFound {
				return util.StatusWrapf(err, "Failed to read previous execution stats with digest %#v", reducedActionDigest.String())
			}
			if handleToReturn.stats.SizeClasses == nil {
				handleToReturn.stats.SizeClasses = map[uint32]*iscc.PerSizeClassStats{}
			}
			return nil
		})
	}

	// Write statistics for the actions that completed previously.
	for _, handleToWriteIter := range handlesToWrite {
		handleToWrite := handleToWriteIter
		group.Go(func() error {
			if err := ss.initialSizeClassCache.Put(ctxWithCancel, handleToWrite.handle.digest, buffer.NewProtoBufferFromProto(handleToWrite.stats, buffer.UserProvided)); err != nil {
				ss.lock.Lock()
				handleToWrite.handle.removeOrQueueForWriteLocked()
				ss.lock.Unlock()
				return util.StatusWrapf(err, "Failed to write previous execution stats with digest %#v", handleToWrite.handle.digest.String())
			}
			ss.lock.Lock()
			handleToWrite.handle.writtenVersion = handleToWrite.writingVersion
			handleToWrite.handle.removeOrQueueForWriteLocked()
			ss.lock.Unlock()
			return nil
		})
	}

	// Wait for the read and both writes to complete.
	if err := group.Wait(); err != nil {
		ss.lock.Lock()
		if hasExistingHandle {
			handleToReturn.decreaseUseCount()
		}
		ss.lock.Unlock()
		return nil, err
	}

	if !hasExistingHandle {
		// Insert the new handle into our bookkeeping. It may be
		// the case that another thread beat us to it. Discard
		// our newly created handle in that case.
		ss.lock.Lock()
		if existingHandle, ok := ss.handles[reducedActionDigest]; ok {
			handleToReturn = existingHandle
			handleToReturn.increaseUseCount()
		} else {
			ss.handles[reducedActionDigest] = handleToReturn
			blobAccessPreviousExecutionStatsHandlesCreated.Inc()
		}
		ss.lock.Unlock()
	}
	return handleToReturn, nil
}

type blobAccessPreviousExecutionStatsHandle struct {
	store  *blobAccessPreviousExecutionStatsStore
	digest digest.Digest

	// The number of times we still expect Release() to be called on
	// the handle.
	useCount int

	// The stats message that all users of the handle mutate. We
	// keep a version number internally to determine whether we
	// need to write the message to storage or discard it.
	stats          iscc.PreviousExecutionStats
	writtenVersion int
	currentVersion int

	// The index of this handle in the handlesToWrite list. We keep
	// track of this index, so that we can remove the handle from
	// the list if needed.
	handlesToWriteIndex int
}

func (sh *blobAccessPreviousExecutionStatsHandle) GetPreviousExecutionStats() *iscc.PreviousExecutionStats {
	return &sh.stats
}

func (sh *blobAccessPreviousExecutionStatsHandle) increaseUseCount() {
	sh.useCount++
	if i := sh.handlesToWriteIndex; i >= 0 {
		// Handle is queued for writing. Remove it, as we'd
		// better write it after further changes have been made.
		ss := sh.store
		newLength := len(ss.handlesToWrite) - 1
		lastHandle := ss.handlesToWrite[newLength]
		ss.handlesToWrite[i] = lastHandle
		if lastHandle.handlesToWriteIndex != newLength {
			panic("Handle has bad write index")
		}
		lastHandle.handlesToWriteIndex = i
		ss.handlesToWrite[newLength] = nil
		ss.handlesToWrite = ss.handlesToWrite[:newLength]
		sh.handlesToWriteIndex = -1
		blobAccessPreviousExecutionStatsHandlesDequeued.Inc()
	}
}

func (sh *blobAccessPreviousExecutionStatsHandle) decreaseUseCount() {
	sh.useCount--
	sh.removeOrQueueForWriteLocked()
}

func (sh *blobAccessPreviousExecutionStatsHandle) removeOrQueueForWriteLocked() {
	if sh.useCount == 0 {
		ss := sh.store
		if sh.writtenVersion == sh.currentVersion {
			// No changes were made to the stats. Simply
			// discard this handle.
			delete(ss.handles, sh.digest)
			blobAccessPreviousExecutionStatsHandlesDestroyed.Inc()
		} else if sh.handlesToWriteIndex < 0 {
			// Changes were made and we're not queued. Place
			// handle in the queue.
			sh.handlesToWriteIndex = len(ss.handlesToWrite)
			ss.handlesToWrite = append(ss.handlesToWrite, sh)
			blobAccessPreviousExecutionStatsHandlesQueued.Inc()
		}
	}
}

func (sh *blobAccessPreviousExecutionStatsHandle) Release(isDirty bool) {
	ss := sh.store
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if isDirty {
		sh.currentVersion = sh.writtenVersion + 1
	}
	sh.decreaseUseCount()
}
