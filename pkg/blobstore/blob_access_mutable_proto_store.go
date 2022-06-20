package blobstore

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	blobAccessMutableProtoHandleMetrics sync.Once

	blobAccessMutableProtoHandlesCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_mutable_proto_handles_created_total",
			Help:      "Number of mutable Protobuf message handles that were created during Get()",
		})
	blobAccessMutableProtoHandlesDestroyed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_mutable_proto_handles_destroyed_total",
			Help:      "Number of mutable Protobuf message handles that were destroyed",
		})
	blobAccessMutableProtoHandlesDequeued = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_mutable_proto_handles_dequeued_total",
			Help:      "Number of mutable Protobuf message handles that were dequeued for writing during Get()",
		})

	blobAccessMutableProtoHandlesQueued = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_mutable_proto_handles_queued_total",
			Help:      "Number of mutable Protobuf message handles that were queued for writing",
		})
)

type blobAccessMutableProtoStore[T any, TProto interface {
	*T
	proto.Message
}] struct {
	initialSizeClassCache   blobstore.BlobAccess
	maximumMessageSizeBytes int

	lock           sync.Mutex
	handles        map[digest.Digest]*blobAccessMutableProtoHandle[T, TProto]
	handlesToWrite []*blobAccessMutableProtoHandle[T, TProto]
}

// NewBlobAccessMutableProtoStore creates an instance of
// MutableProtoStore that is backed by BlobAccess.
//
// What makes this interface harder to implement is that releasing
// MutableProtoHandle is performed while holding locks. We can't block,
// nor can we propagate errors or perform retries. To solve this, this
// implementation keeps track of a list of all handles that need to be
// written. Every time a handle is created, we write a couple of
// released handles back to storage. This ensures that the number of
// handles remains proportional to actual use.
func NewBlobAccessMutableProtoStore[T any, TProto interface {
	*T
	proto.Message
}](initialSizeClassCache blobstore.BlobAccess, maximumMessageSizeBytes int) MutableProtoStore[TProto] {
	blobAccessMutableProtoHandleMetrics.Do(func() {
		prometheus.MustRegister(blobAccessMutableProtoHandlesCreated)
		prometheus.MustRegister(blobAccessMutableProtoHandlesDestroyed)
		prometheus.MustRegister(blobAccessMutableProtoHandlesDequeued)
		prometheus.MustRegister(blobAccessMutableProtoHandlesQueued)
	})

	return &blobAccessMutableProtoStore[T, TProto]{
		initialSizeClassCache:   initialSizeClassCache,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		handles:                 map[digest.Digest]*blobAccessMutableProtoHandle[T, TProto]{},
	}
}

type handleToWrite[T any, TProto interface {
	*T
	proto.Message
}] struct {
	handle         *blobAccessMutableProtoHandle[T, TProto]
	message        proto.Message
	writingVersion int
}

func (ss *blobAccessMutableProtoStore[T, TProto]) Get(ctx context.Context, reducedActionDigest digest.Digest) (MutableProtoHandle[TProto], error) {
	const writesPerRead = 3
	handlesToWrite := make([]handleToWrite[T, TProto], 0, writesPerRead)

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
	// handle.message here, as handle.useCount is guaranteed to be
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
		handlesToWrite = append(handlesToWrite, handleToWrite[T, TProto]{
			handle:         handle,
			message:        proto.Clone(TProto(&handle.message)),
			writingVersion: handle.currentVersion,
		})
	}
	ss.lock.Unlock()
	blobAccessMutableProtoHandlesDequeued.Add(float64(len(handlesToWrite)))

	// If no handle exists, create a new handle containing the
	// existing message for the action.
	group, ctxWithCancel := errgroup.WithContext(ctx)
	if !hasExistingHandle {
		handleToReturn = &blobAccessMutableProtoHandle[T, TProto]{
			store:               ss,
			digest:              reducedActionDigest,
			useCount:            1,
			handlesToWriteIndex: -1,
		}
		group.Go(func() error {
			emptyMessage := new(T)
			if m, err := ss.initialSizeClassCache.Get(ctxWithCancel, reducedActionDigest).ToProto(TProto(emptyMessage), ss.maximumMessageSizeBytes); err == nil {
				proto.Merge(TProto(&handleToReturn.message), m)
			} else if status.Code(err) != codes.NotFound {
				return util.StatusWrapf(err, "Failed to read mutable Protobuf message with digest %#v", reducedActionDigest.String())
			}
			return nil
		})
	}

	// Write statistics for the actions that completed previously.
	for _, handleToWriteIter := range handlesToWrite {
		handleToWrite := handleToWriteIter
		group.Go(func() error {
			if err := ss.initialSizeClassCache.Put(ctxWithCancel, handleToWrite.handle.digest, buffer.NewProtoBufferFromProto(handleToWrite.message, buffer.UserProvided)); err != nil {
				ss.lock.Lock()
				handleToWrite.handle.removeOrQueueForWriteLocked()
				ss.lock.Unlock()
				return util.StatusWrapf(err, "Failed to write mutable Protobuf message with digest %#v", handleToWrite.handle.digest.String())
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
			blobAccessMutableProtoHandlesCreated.Inc()
		}
		ss.lock.Unlock()
	}
	return handleToReturn, nil
}

type blobAccessMutableProtoHandle[T any, TProto interface {
	*T
	proto.Message
}] struct {
	store  *blobAccessMutableProtoStore[T, TProto]
	digest digest.Digest

	// The number of times we still expect Release() to be called on
	// the handle.
	useCount int

	// The message that all users of the handle mutate. We keep a
	// version number internally to determine whether we need to
	// write the message to storage or discard it.
	message        T
	writtenVersion int
	currentVersion int

	// The index of this handle in the handlesToWrite list. We keep
	// track of this index, so that we can remove the handle from
	// the list if needed.
	handlesToWriteIndex int
}

func (sh *blobAccessMutableProtoHandle[T, TProto]) GetMutableProto() TProto {
	return TProto(&sh.message)
}

func (sh *blobAccessMutableProtoHandle[T, TProto]) increaseUseCount() {
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
		blobAccessMutableProtoHandlesDequeued.Inc()
	}
}

func (sh *blobAccessMutableProtoHandle[T, TProto]) decreaseUseCount() {
	sh.useCount--
	sh.removeOrQueueForWriteLocked()
}

func (sh *blobAccessMutableProtoHandle[T, TProto]) removeOrQueueForWriteLocked() {
	if sh.useCount == 0 {
		ss := sh.store
		if sh.writtenVersion == sh.currentVersion {
			// No changes were made to the message. Simply
			// discard this handle.
			delete(ss.handles, sh.digest)
			blobAccessMutableProtoHandlesDestroyed.Inc()
		} else if sh.handlesToWriteIndex < 0 {
			// Changes were made and we're not queued. Place
			// handle in the queue.
			sh.handlesToWriteIndex = len(ss.handlesToWrite)
			ss.handlesToWrite = append(ss.handlesToWrite, sh)
			blobAccessMutableProtoHandlesQueued.Inc()
		}
	}
}

func (sh *blobAccessMutableProtoHandle[T, TProto]) Release(isDirty bool) {
	ss := sh.store
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if isDirty {
		sh.currentVersion = sh.writtenVersion + 1
	}
	sh.decreaseUseCount()
}
