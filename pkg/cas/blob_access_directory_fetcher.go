package cas

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

type blobAccessDirectoryFetcher struct {
	blobAccess           blobstore.BlobAccess
	slicer               treeBlobSlicer
	maximumTreeSizeBytes int64
}

// NewBlobAccessDirectoryFetcher creates a DirectoryFetcher that reads
// Directory objects from a BlobAccess based store.
func NewBlobAccessDirectoryFetcher(blobAccess blobstore.BlobAccess, maximumDirectorySizeBytes int, maximumTreeSizeBytes int64) DirectoryFetcher {
	return &blobAccessDirectoryFetcher{
		blobAccess: blobAccess,
		slicer: treeBlobSlicer{
			maximumDirectorySizeBytes: maximumDirectorySizeBytes,
		},
		maximumTreeSizeBytes: maximumTreeSizeBytes,
	}
}

func (df *blobAccessDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.blobAccess.Get(ctx, directoryDigest).ToProto(&remoteexecution.Directory{}, df.slicer.maximumDirectorySizeBytes)
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}

func (df *blobAccessDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	if treeDigest.GetSizeBytes() > df.maximumTreeSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Tree exceeds the maximum permitted size of %d bytes", df.maximumTreeSizeBytes)
	}

	r := df.blobAccess.Get(ctx, treeDigest).ToReader()
	defer r.Close()

	var rootDirectory *remoteexecution.Directory
	if err := util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber == blobstore.TreeRootFieldNumber {
			if rootDirectory != nil {
				return status.Error(codes.InvalidArgument, "Tree contains multiple root directories")
			}
			m, err := buffer.NewProtoBufferFromReader(
				&remoteexecution.Directory{},
				io.NopCloser(fieldReader),
				buffer.UserProvided,
			).ToProto(&remoteexecution.Directory{}, df.slicer.maximumDirectorySizeBytes)
			if err != nil {
				return err
			}
			rootDirectory = m.(*remoteexecution.Directory)
		}
		return nil
	}); err != nil {
		if _, copyErr := io.Copy(io.Discard, r); copyErr != nil {
			copyErr = err
		}
		return nil, err
	}
	if rootDirectory == nil {
		return nil, status.Error(codes.InvalidArgument, "Tree does not contain a root directory")
	}
	return rootDirectory, nil
}

func (df *blobAccessDirectoryFetcher) GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error) {
	if treeDigest.GetSizeBytes() > df.maximumTreeSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Tree exceeds the maximum permitted size of %d bytes", df.maximumTreeSizeBytes)
	}

	m, err := df.blobAccess.GetFromComposite(ctx, treeDigest, childDigest, &df.slicer).ToProto(&remoteexecution.Directory{}, df.slicer.maximumDirectorySizeBytes)
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}

// treeBlobSlicer is capable of unpacking an REv2 Tree object stored in
// the Content Addressable Storage (CAS) into separate Directory
// objects. This allows implementations of BlobAccess to store the
// contents of the Tree just once, but to create entries in its index
// that refer to each of the Directories contained within.
type treeBlobSlicer struct {
	maximumDirectorySizeBytes int
}

func (bs *treeBlobSlicer) Slice(b buffer.Buffer, requestedChildDigest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
	r := b.ToReader()
	defer r.Close()

	requestedSizeBytes := requestedChildDigest.GetSizeBytes()
	digestFunction := requestedChildDigest.GetDigestFunction()
	var slices []slicing.BlobSlice
	var bRequested buffer.Buffer
	if err := util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber == blobstore.TreeChildrenFieldNumber {
			var childDigest digest.Digest
			if bRequested == nil && sizeBytes == requestedSizeBytes {
				// This directory has the same size as
				// the one that is requested, so we may
				// need to return it. Duplicate it.
				b1, b2 := buffer.NewProtoBufferFromReader(
					&remoteexecution.Directory{},
					io.NopCloser(fieldReader),
					buffer.UserProvided,
				).CloneCopy(bs.maximumDirectorySizeBytes)

				childDigestGenerator := digestFunction.NewGenerator()
				if err := b1.IntoWriter(childDigestGenerator); err != nil {
					b2.Discard()
					return err
				}
				childDigest = childDigestGenerator.Sum()

				if childDigest == requestedChildDigest {
					// Found the directory that was
					// requested. Return it.
					bRequested = b2
				} else {
					b2.Discard()
				}
			} else {
				// The directory's size doesn't match,
				// so we can compute its checksum
				// without unmarshaling it.
				childDigestGenerator := digestFunction.NewGenerator()
				if _, err := io.Copy(childDigestGenerator, fieldReader); err != nil {
					return err
				}
				childDigest = childDigestGenerator.Sum()
			}
			slices = append(slices, slicing.BlobSlice{
				Digest:      childDigest,
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			})
		}
		return nil
	}); err != nil {
		if bRequested != nil {
			bRequested.Discard()
		}
		if _, copyErr := io.Copy(io.Discard, r); copyErr != nil {
			copyErr = err
		}
		return buffer.NewBufferFromError(err), nil
	}
	if bRequested == nil {
		bRequested = buffer.NewBufferFromError(status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree"))
	}
	return bRequested, slices
}
