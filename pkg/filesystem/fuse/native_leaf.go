// +build darwin linux

package fuse

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NativeLeaf objects are non-directory nodes that can be placed in an
// InMemoryDirectory. Examples of leaves are files and symbolic links.
// As InMemoryDirectory can both be accessed as a filesystem.Directory
// (by bb-worker itself) or through FUSE (by build actions spawned by
// bb-runner), NativeLeaf nodes need to implement two sets of
// operations.
type NativeLeaf interface {
	Leaf

	GetFileType() filesystem.FileType
	Link()
	Readlink() (string, error)
	Unlink()
	UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error)

	FUSEGetDirEntry() fuse.DirEntry
}
