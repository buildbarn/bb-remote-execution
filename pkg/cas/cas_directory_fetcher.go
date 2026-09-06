package cas

import (
	"context"
	"errors"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// errTargetFound is a sentinel error used to abort the protobuf field
// iteration once we've found the target directory we are looking for.
var errTargetFound = errors.New("target directory found")

type casDirectoryFetcher struct {
	contentAddressableStorage cas.ContentAddressableStorage
	directoryReader           cas.MessageReader[*remoteexecution.Directory]
	treeReader                cas.StreamReader
	maximumTreeSizeBytes      int64
	maximumDirectorySizeBytes int64
}

// NewCASDirectoryFetcher creates a DirectoryFetcher that reads
// Directory objects from a BlobAccess based store.
func NewCASDirectoryFetcher(contentAddressableStorage cas.ContentAddressableStorage, directoryReader cas.MessageReader[*remoteexecution.Directory], treeReader cas.StreamReader, maximumDirectorySizeBytes, maximumTreeSizeBytes int64) DirectoryFetcher {
	return &casDirectoryFetcher{
		contentAddressableStorage: contentAddressableStorage,
		directoryReader:           directoryReader,
		treeReader:                treeReader,
		maximumTreeSizeBytes:      maximumTreeSizeBytes,
		maximumDirectorySizeBytes: maximumDirectorySizeBytes,
	}
}

func (df *casDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.directoryReader.ReadMessage(ctx, directoryDigest)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (df *casDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	if treeDigest.GetSizeBytes() > df.maximumTreeSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Tree exceeds the maximum permitted size of %d bytes", df.maximumTreeSizeBytes)
	}

	r, err := df.treeReader.ReadStream(ctx, treeDigest)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var rootDirectory *remoteexecution.Directory
	err = util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber != blobstore.TreeRootFieldNumber {
			return nil
		}

		if sizeBytes > df.maximumDirectorySizeBytes {
			return status.Errorf(codes.InvalidArgument, "Root directory exceeds the maximum permitted size of %d bytes", df.maximumDirectorySizeBytes)
		}

		dirBytes := make([]byte, sizeBytes)
		if _, err := io.ReadFull(fieldReader, dirBytes); err != nil {
			return err
		}

		var dir remoteexecution.Directory
		if err := proto.Unmarshal(dirBytes, &dir); err != nil {
			return util.StatusWrap(err, "Failed to unmarshal root directory")
		}

		rootDirectory = &dir
		return errTargetFound
	})

	if rootDirectory == nil {
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.InvalidArgument, "Tree does not contain a root directory")
	}

	return rootDirectory, nil
}

func (df *casDirectoryFetcher) GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error) {
	if treeDigest.GetSizeBytes() > df.maximumTreeSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Tree exceeds the maximum permitted size of %d bytes", df.maximumTreeSizeBytes)
	}
	if childDigest.GetSizeBytes() > df.maximumDirectorySizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Child digest exceeds the maximum permitted size of %d bytes", df.maximumDirectorySizeBytes)
	}

	r, err := df.treeReader.ReadStream(ctx, treeDigest)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var foundDirectory *remoteexecution.Directory
	digestFunction := childDigest.GetDigestFunction()
	err = util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber != blobstore.TreeRootFieldNumber && fieldNumber != blobstore.TreeChildrenFieldNumber {
			return nil
		}
		if sizeBytes != childDigest.GetSizeBytes() {
			return nil
		}

		dirBytes := make([]byte, sizeBytes)
		if _, err := io.ReadFull(fieldReader, dirBytes); err != nil {
			return err
		}

		generator := digestFunction.NewGenerator(sizeBytes)
		if _, err := generator.Write(dirBytes); err != nil {
			return err
		}

		if generator.Sum() != childDigest {
			return nil
		}

		var dir remoteexecution.Directory
		if err := proto.Unmarshal(dirBytes, &dir); err != nil {
			return util.StatusWrap(err, "Failed to unmarshal child directory")
		}

		foundDirectory = &dir
		return errTargetFound
	})

	if foundDirectory == nil {
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree")
	}

	return foundDirectory, nil
}
