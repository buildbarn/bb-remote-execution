package access

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
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
	ResolvedDirectory(name path.Component) UnreadDirectoryMonitor
	ReadFile(name path.Component)
}
