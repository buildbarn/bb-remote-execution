package cas

import (
	"bytes"
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blobAccessDirectoryFetcher struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
	slicer                  treeBlobSlicer
}

// NewBlobAccessDirectoryFetcher creates a DirectoryFetcher that reads
// Directory objects from a BlobAccess based store.
func NewBlobAccessDirectoryFetcher(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) DirectoryFetcher {
	return &blobAccessDirectoryFetcher{
		blobAccess: blobAccess,
		slicer: treeBlobSlicer{
			maximumMessageSizeBytes: maximumMessageSizeBytes,
		},
	}
}

func (df *blobAccessDirectoryFetcher) GetDirectory(ctx context.Context, directoryDigest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.blobAccess.Get(ctx, directoryDigest).ToProto(&remoteexecution.Directory{}, df.slicer.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}

func (df *blobAccessDirectoryFetcher) GetTreeRootDirectory(ctx context.Context, treeDigest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.blobAccess.Get(ctx, treeDigest).ToProto(&remoteexecution.Tree{}, df.slicer.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	tree := m.(*remoteexecution.Tree)
	if tree.Root == nil {
		return nil, status.Error(codes.InvalidArgument, "Tree does not contain a root directory")
	}
	return tree.Root, nil
}

func (df *blobAccessDirectoryFetcher) GetTreeChildDirectory(ctx context.Context, treeDigest, childDigest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := df.blobAccess.GetFromComposite(ctx, treeDigest, childDigest, &df.slicer).ToProto(&remoteexecution.Directory{}, df.slicer.maximumMessageSizeBytes)
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
	maximumMessageSizeBytes int
}

func (bs *treeBlobSlicer) Slice(b buffer.Buffer, requestedChildDigest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
	// Fetch the Tree object, both in binary and message form.
	bData, bMessage := b.CloneCopy(bs.maximumMessageSizeBytes)
	treeData, err := bData.ToByteSlice(bs.maximumMessageSizeBytes)
	if err != nil {
		bMessage.Discard()
		return buffer.NewBufferFromError(err), nil
	}
	treeMessage, err := bMessage.ToProto(&remoteexecution.Tree{}, bs.maximumMessageSizeBytes)
	if err != nil {
		return buffer.NewBufferFromError(err), nil
	}
	tree := treeMessage.(*remoteexecution.Tree)

	digestFunction := requestedChildDigest.GetDigestFunction()
	slices := make([]slicing.BlobSlice, 0, len(tree.Children))
	treeDataOffset := 0
	var bRequested buffer.Buffer
	for childIndex, child := range tree.Children {
		bData := buffer.NewProtoBufferFromProto(child, buffer.UserProvided)
		setRequested := bRequested == nil
		if setRequested {
			bData, bRequested = bData.CloneCopy(bs.maximumMessageSizeBytes)
		}

		childData, err := bData.ToByteSlice(bs.maximumMessageSizeBytes)
		if err != nil {
			bRequested.Discard()
			return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.InvalidArgument, "Child directory at index %d", childIndex)), slices
		}

		// Obtain the region at which the Directory message is
		// stored within the Tree message.
		skipBytes := bytes.Index(treeData, childData)
		if skipBytes < 0 {
			bRequested.Discard()
			return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.InvalidArgument, "Child directory at index %d is not in canonical form", childIndex)), slices
		}
		childOffsetBytes := treeDataOffset + skipBytes

		// Assume that Directory objects in the marshaled Tree
		// object are stored in the same order as the
		// unmarshaled list. This permits bytes.Index() calls to
		// run in linear amortized time.
		treeData = treeData[skipBytes+len(childData):]
		treeDataOffset += skipBytes + len(childData)

		// Create a slice for the Directory.
		childDigestGenerator := digestFunction.NewGenerator()
		if _, err := childDigestGenerator.Write(childData); err != nil {
			panic(err)
		}
		childDigest := childDigestGenerator.Sum()
		slices = append(slices, slicing.BlobSlice{
			Digest:      childDigestGenerator.Sum(),
			OffsetBytes: int64(childOffsetBytes),
			SizeBytes:   int64(len(childData)),
		})

		if setRequested && childDigest != requestedChildDigest {
			// Current Directory is not the one that was
			// requested by the caller.
			bRequested.Discard()
			bRequested = nil
		}
	}

	if bRequested == nil {
		bRequested = buffer.NewBufferFromError(status.Error(codes.InvalidArgument, "Requested child directory is not contained in the tree"))
	}
	return bRequested, slices
}
