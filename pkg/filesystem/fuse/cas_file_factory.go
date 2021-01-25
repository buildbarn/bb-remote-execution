// +build darwin linux

package fuse

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// CASFileFactory is a factory type for files whose contents correspond
// with an object stored in the Content Addressable Storage (CAS).
type CASFileFactory interface {
	GetFileInodeNumber(digest digest.Digest, isExecutable bool) uint64
	LookupFile(digest digest.Digest, isExecutable bool, out *fuse.Attr) NativeLeaf
}
