//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NativeLeaf objects are non-directory nodes that can be placed in a
// PrepopulatedDirectory.
type NativeLeaf interface {
	Leaf

	// Operations called into by implementations of
	// PrepopulatedDirectory.
	GetFileType() filesystem.FileType
	Link()
	Unlink()
	FUSEGetDirEntry() fuse.DirEntry

	// Additional operations that are used by consumers of
	// PrepopulatedDirectory.
	//
	// TODO: Remove these once Go supports generics. We could turn
	// PrepopulatedDirectory and InitialContentsFetcher into
	// parameterized types, where the leaves could be any type
	// that's based on NativeLeaf.
	Readlink() (string, error)
	UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error)
	// GetContainingDigests() returns a set of digests of objects in
	// the Content Addressable Storage that back the contents of
	// this file.
	//
	// The set returned by this function may be passed to
	// ContentAddressableStorage.FindMissingBlobs() to check whether
	// the file still exists in its entirety, and to prevent that
	// the file is removed in the nearby future.
	GetContainingDigests() digest.Set
	// GetOutputServiceFileStatus() returns the status of the leaf
	// node in the form of a FileStatus message that is used by the
	// Remote Output Service protocol.
	//
	// When digestFunction is not nil, a FileStatus responses for
	// regular files should include the digest.
	GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error)
	// GetOutputServiceFileStatus() appends a FileNode or
	// SymlinkNode entry to a Directory message that is used to
	// persist the state of a Remote Output Service output path to
	// disk.
	AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component)
}
