package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// InitialNode is the value type of the map of directory entries
// returned by InitialContentsFetcher.FetchContents(). Either Directory
// or Leaf is set, but not both.
type InitialNode struct {
	Directory InitialContentsFetcher
	Leaf      NativeLeaf
}

// InitialContentsFetcher is called into by PrepopulatedDirectory when a
// directory whose contents need to be instantiated lazily is accessed.
// The results returned by FetchContents() are used to populate the
// directory.
//
// FetchContents() should be called until it succeeds at most once. It
// may be possible FetchContents() is never called. This may happen if
// the directory in question is never accessed.
type InitialContentsFetcher interface {
	FetchContents() (map[path.Component]InitialNode, error)

	// GetContainingDigests() returns a set of digests of objects in
	// the Content Addressable Storage that back the directories and
	// leaf nodes yielded by this InitialContentsFetcher.
	//
	// The set returned by this function may be passed to
	// ContentAddressableStorage.FindMissingBlobs() to check whether
	// the all files underneath this directory still exist, and to
	// prevent them from being removed in the nearby future.
	//
	// This API assumes that the resulting set is small enough to
	// fit in memory. For hierarchies backed by Tree objects, this
	// will generally hold. It may not be safe to call this method
	// on InitialContentsFetchers that expand to infinitely big
	// hierarchies.
	GetContainingDigests(ctx context.Context) (digest.Set, error)
}
