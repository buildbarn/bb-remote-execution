// +build darwin linux

package fuse

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// InitialContentsFetcher is called into by InMemoryDirectory when a
// directory whose contents need to be instantiated lazily is accessed.
// The results returned by FetchContents() are used to populate the
// directory.
//
// FetchContents() should be called until it succeeds at most once. It
// may be possible FetchContents() is never called. This may happen if
// the directory in question is never accessed.
type InitialContentsFetcher interface {
	FetchContents() (map[path.Component]InitialContentsFetcher, map[path.Component]NativeLeaf, error)
}
