package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
)

type readWriteDecouplingContentAddressableStorage struct {
	reader cas.ContentAddressableStorage
	writer cas.ContentAddressableStorage
}

// NewReadWriteDecouplingContentAddressableStorage takes a pair of
// ContentAddressableStorage objects and forwards reads and write
// requests to them, respectively. It can, for example, be used to
// forward read requests to a process-wide cache, while write requests
// are sent to a worker/action-specific write cache.
func NewReadWriteDecouplingContentAddressableStorage(reader cas.ContentAddressableStorage, writer cas.ContentAddressableStorage) cas.ContentAddressableStorage {
	return &readWriteDecouplingContentAddressableStorage{
		reader: reader,
		writer: writer,
	}
}

func (cas *readWriteDecouplingContentAddressableStorage) GetAction(ctx context.Context, digest digest.Digest) (*remoteexecution.Action, error) {
	return cas.reader.GetAction(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetCommand(ctx context.Context, digest digest.Digest) (*remoteexecution.Command, error) {
	return cas.reader.GetCommand(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	return cas.reader.GetDirectory(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	return cas.reader.GetFile(ctx, digest, directory, name, isExecutable)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetTree(ctx context.Context, digest digest.Digest) (*remoteexecution.Tree, error) {
	return cas.reader.GetTree(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetUncachedActionResult(ctx context.Context, digest digest.Digest) (*cas_proto.UncachedActionResult, error) {
	return cas.reader.GetUncachedActionResult(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.writer.PutFile(ctx, directory, name, parentDigest)
}

func (cas *readWriteDecouplingContentAddressableStorage) PutLog(ctx context.Context, log []byte, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.writer.PutLog(ctx, log, parentDigest)
}

func (cas *readWriteDecouplingContentAddressableStorage) PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.writer.PutTree(ctx, tree, parentDigest)
}

func (cas *readWriteDecouplingContentAddressableStorage) PutUncachedActionResult(ctx context.Context, uncachedActionResult *cas_proto.UncachedActionResult, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.writer.PutUncachedActionResult(ctx, uncachedActionResult, parentDigest)
}
