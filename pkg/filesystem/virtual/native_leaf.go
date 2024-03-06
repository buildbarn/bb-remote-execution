package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// NativeLeaf objects are non-directory nodes that can be placed in a
// PrepopulatedDirectory.
type NativeLeaf interface {
	Leaf

	// Operations called into by implementations of
	// PrepopulatedDirectory. The Link() operation may fail, for the
	// reason that Directory.VirtualLink() may be called on leaf
	// nodes that have been removed concurrently.
	Link() Status
	Unlink()

	// Additional operations that are used by consumers of
	// PrepopulatedDirectory.
	//
	// TODO: Remove these once Go supports generics. We could turn
	// PrepopulatedDirectory and InitialContentsFetcher into
	// parameterized types, where the leaves could be any type
	// that's based on NativeLeaf.
	Readlink() (string, error)
	UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error)
	// GetContainingDigests() returns a set of digests of objects in
	// the Content Addressable Storage that back the contents of
	// this file.
	//
	// The set returned by this function may be passed to
	// ContentAddressableStorage.FindMissingBlobs() to check whether
	// the file still exists in its entirety, and to prevent that
	// the file is removed in the nearby future.
	GetContainingDigests() digest.Set
	// GetBazelOutputServiceStat() returns the status of the leaf
	// node in the form of a Status message that is used by the
	// Bazel Output Service protocol.
	GetBazelOutputServiceStat(digestFunction *digest.Function) (*bazeloutputservice.BatchStatResponse_Stat, error)
	// AppendOutputPathPersistencyDirectoryNode() appends a FileNode
	// or SymlinkNode entry to a Directory message that is used to
	// persist the state of a Bazel Output Service output path to
	// disk.
	AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component)
}
