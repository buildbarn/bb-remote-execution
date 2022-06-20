package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

// MutableProtoStore is a store for Protobuf messages, allowing them
// both to be read and written. Because multiple operations may interact
// with a single Protobuf message, this interface permits concurrent
// access to the same message.
//
// The Get() function may be called in parallel, yielding a
// MutableProtoHandle. Because these handles are shared, all methods on
// all handles obtained from a single store must be called while holding
// a global lock. The Protobuf message embedded in the handle gets
// invalidated after locks are dropped.
type MutableProtoStore[T proto.Message] interface {
	Get(ctx context.Context, reducedActionDigest digest.Digest) (MutableProtoHandle[T], error)
}

// MutableProtoHandle is a handle that is returned by MutableProtoStore.
// It contains a MutableProto message that contains timing information
// of previous executions of similar actions.
type MutableProtoHandle[T proto.Message] interface {
	GetMutableProto() T
	Release(isDirty bool)
}
