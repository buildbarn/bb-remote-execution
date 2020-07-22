package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type readWriteDecouplingContentAddressableStorage struct {
	reader ContentAddressableStorage
	writer ContentAddressableStorage
}

// NewReadWriteDecouplingContentAddressableStorage takes a pair of
// ContentAddressableStorage objects and forwards reads and write
// requests to them, respectively. It can, for example, be used to
// forward read requests to a process-wide cache, while write requests
// are sent to a worker/action-specific write cache.
func NewReadWriteDecouplingContentAddressableStorage(reader ContentAddressableStorage, writer ContentAddressableStorage) ContentAddressableStorage {
	return &readWriteDecouplingContentAddressableStorage{
		reader: reader,
		writer: writer,
	}
}

func (cas *readWriteDecouplingContentAddressableStorage) GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	return cas.reader.GetDirectory(ctx, digest)
}

func (cas *readWriteDecouplingContentAddressableStorage) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	return cas.reader.GetFile(ctx, digest, directory, name, isExecutable)
}

func (cas *readWriteDecouplingContentAddressableStorage) PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.writer.PutFile(ctx, directory, name, parentDigest)
}
