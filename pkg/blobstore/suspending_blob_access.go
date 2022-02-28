package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Suspendable object that is used by NewSuspendingBlobAccess().
// Examples of suspendable objects include SuspendableClock.
type Suspendable interface {
	Suspend()
	Resume()
}

type suspendingBlobAccess struct {
	base        blobstore.BlobAccess
	suspendable Suspendable
}

// NewSuspendingBlobAccess is a decorator for BlobAccess that simply
// forwards all methods. Before and after each call, it suspends and
// resumes a Suspendable object, respectively.
//
// This decorator is used in combination with SuspendableClock, allowing
// FUSE-based workers to compensate the execution timeout of build
// actions for any time spent downloading the input root.
func NewSuspendingBlobAccess(base blobstore.BlobAccess, suspendable Suspendable) blobstore.BlobAccess {
	return &suspendingBlobAccess{
		base:        base,
		suspendable: suspendable,
	}
}

func (ba *suspendingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	ba.suspendable.Suspend()
	return buffer.WithErrorHandler(
		ba.base.Get(ctx, digest),
		&resumingErrorHandler{suspendable: ba.suspendable})
}

func (ba *suspendingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()

	return ba.base.Put(ctx, digest, b)
}

func (ba *suspendingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()

	return ba.base.FindMissing(ctx, digests)
}

func (ba *suspendingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	ba.suspendable.Suspend()
	defer ba.suspendable.Resume()

	return ba.base.GetCapabilities(ctx, instanceName)
}

type resumingErrorHandler struct {
	suspendable Suspendable
}

func (eh *resumingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, err
}

func (eh *resumingErrorHandler) Done() {
	eh.suspendable.Resume()
}
