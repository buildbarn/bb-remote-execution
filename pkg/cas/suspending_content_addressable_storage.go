package cas

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type suspendingContentAddressableStorage struct {
	base        cdc.ContentAddressableStorage
	suspendable clock.Suspendable
}

// NewSuspendingContentAddressableStorage is a decorator for a
// ContentAddressableStorage that simply forwards all methods. Before
// and after each call, it suspends and resumes a clock.Suspendable
// object, respectively.
//
// This decorator is used in combination with SuspendableClock, allowing
// VFS-based workers to compensate the execution timeout of build
// actions for any time spent downloading the input root.
func NewSuspendingContentAddressableStorage(base cdc.ContentAddressableStorage, suspendable clock.Suspendable) cdc.ContentAddressableStorage {
	return &suspendingContentAddressableStorage{
		base:        base,
		suspendable: suspendable,
	}
}

func (cas suspendingContentAddressableStorage) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (cdc.Parameters, error) {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.FetchCDCParameters(ctx, instanceName)
}

func (cas suspendingContentAddressableStorage) GetDigestKeyFormat() digest.KeyFormat {
	return cas.base.GetDigestKeyFormat()
}

func (cas suspendingContentAddressableStorage) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.FindMissing(ctx, digests)
}

func (cas suspendingContentAddressableStorage) FetchChunk(ctx context.Context, d digest.Digest) ([]byte, error) {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.FetchChunk(ctx, d)
}

func (cas suspendingContentAddressableStorage) PutChunk(ctx context.Context, d digest.Digest, data []byte) error {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.PutChunk(ctx, d, data)
}

func (cas suspendingContentAddressableStorage) GetManifest(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.GetManifest(ctx, d)
}

func (cas suspendingContentAddressableStorage) PutManifest(ctx context.Context, d digest.Digest, manifest chunklist.ChunkList) error {
	cas.suspendable.Suspend()
	defer cas.suspendable.Resume()
	return cas.base.PutManifest(ctx, d, manifest)
}
