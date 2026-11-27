package access

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// UnreadDirectoryMonitor is used to report file system access activity
// against a directory whose contents have not yet been read.
type UnreadDirectoryMonitor interface {
	ReadDirectory() ReadDirectoryMonitor
}

// ReadDirectoryMonitor is used to report file system access activity
// against a directory whose contents have been read. It is possible to
// report resolution of child directories, and reads against child
// files.
type ReadDirectoryMonitor interface {
	ResolvedDirectory(directoryDigest digest.Digest) UnreadDirectoryMonitor
	ReadFile(fileDigest digest.Digest)
}
