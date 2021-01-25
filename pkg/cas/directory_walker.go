package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// DirectoryWalker is identical to a DirectoryFetcher, except that it is
// bound to a specific instance of a directory.
//
// The goal of this interface is to provide uniform access to Directory
// messages, regardless of the way they are stored in the Content
// Addressable Storage (i.e., as separate objects, or as part of a Tree
// message).
type DirectoryWalker interface {
	// GetDirectory() returns the contents of the current directory.
	GetDirectory(ctx context.Context) (*remoteexecution.Directory, error)

	// GetChild() can be used obtain a new DirectoryWalker instance
	// that corresponds to one of the children of this directory.
	GetChild(digest digest.Digest) DirectoryWalker

	// GetDescription() gives a textual description of the
	// DirectoryWalker, which may be useful for logging purposes.
	GetDescription() string

	// GetContainingDigest() returns the digest of the Content
	// Addressable Storage object that holds this directory.
	//
	// In the case of plain Directory objects, this function returns
	// the digest provided to GetChild(). In the case of Tree
	// objects, the digest of the containing Tree is returned, which
	// differs from the digest provided to GetChild().
	GetContainingDigest() digest.Digest
}
