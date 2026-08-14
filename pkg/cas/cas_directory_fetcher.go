package cas

import (
	"context"
	"errors"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// errTargetFound is a sentinel error used to cleanly abort the protobuf
// field iteration once we've found the target directory we are looking
// for.
var errTargetFound = errors.New("target directory found")

type casDirectoryFetcher struct {
	contentAddressableStorage cdc.ContentAddressableStorage
	maximumTreeSizeBytes      int64
	maximumDirectorySizeBytes int64
}

// NewCASDirectoryFetcher creates a DirectoryFetcher that reads Directory
// objects from a CAS.
func NewCASDirectoryFetcher(contentAddressableStorage cdc.ContentAddressableStorage, maximumDirectorySizeBytes, maximumTreeSizeBytes int64) DirectoryFetcher {
	return &casDirectoryFetcher{
		contentAddressableStorage: contentAddressableStorage,
		maximumDirectorySizeBytes: maximumDirectorySizeBytes,
		maximumTreeSizeBytes:      maximumTreeSizeBytes,
	}
}

func (df *casDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	if directoryDigest.GetSizeBytes() > df.maximumDirectorySizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Directory exceeds the maximum permitted size of %d bytes", df.maximumDirectorySizeBytes)
	}

	m, err := cdc.GetProto(ctx, df.contentAddressableStorage, directoryDigest, &remoteexecution.Directory{})
	if err != nil {
		return nil, err
	}

	return m, nil
}

// streamTree handles the common boilerplate of opening a tree stream
// from the CAS, checking limits, parsing its fields, and ensuring
// proper teardown. It intercepts errTargetFound to allow callers to
// short-circuit cleanly.
func (df *casDirectoryFetcher) streamTree(ctx context.Context, treeDigest digest.Digest, visitor func(protowire.Number, int64, int64, io.Reader) error) error {
	if treeDigest.GetSizeBytes() > df.maximumTreeSizeBytes {
		return status.Errorf(codes.InvalidArgument, "Tree exceeds the maximum permitted size of %d bytes", df.maximumTreeSizeBytes)
	}

	r, err := cdc.GetReadCloser(ctx, df.contentAddressableStorage, treeDigest)
	if err != nil {
		return err
	}
	defer r.Close()

	return util.VisitProtoBytesFields(r, visitor)
}

func (df *casDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	var rootDirectory *remoteexecution.Directory

	err := df.streamTree(ctx, treeDigest, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
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
	directorySizeBytes := childDigest.GetSizeBytes()
	if directorySizeBytes > df.maximumDirectorySizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Requested child directory exceeds the maximum permitted size of %d bytes", df.maximumDirectorySizeBytes)
	}

	var foundDirectory *remoteexecution.Directory
	digestFunction := childDigest.GetDigestFunction()

	err := df.streamTree(ctx, treeDigest, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber != blobstore.TreeRootFieldNumber && fieldNumber != blobstore.TreeChildrenFieldNumber {
			return nil
		}
		if sizeBytes != directorySizeBytes {
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
