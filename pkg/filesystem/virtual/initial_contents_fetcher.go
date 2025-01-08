package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// InitialNode contains the common set of operations that can be applied
// against files and directories returned by
// InitialContentsFetcher.FetchContents().
type InitialNode interface {
	// VirtualApply can be used to perform implementation defined
	// operations against files and directories.
	VirtualApply(data any) bool
}

// InitialChild is the value type of the map of directory entries
// returned by InitialContentsFetcher.FetchContents(). Either Directory
// or Leaf is set, but not both.
type InitialChild = Child[InitialContentsFetcher, LinkableLeaf, InitialNode]

// FileReadMonitor is used by the regular files created through the
// InitialContentsFetcher to indicate that one or more calls against
// VirtualRead() have occurred. This is used by
// AccessMonitoringInitialContentsFetcher to monitor file access.
type FileReadMonitor func()

// FileReadMonitorFactory is a factory type for FileReadMonitor that is
// provided to the InitialContentsFetcher, so that the
// InitialContentsFetcher can attach the resulting monitors to any files
// that are returned.
//
// If this function returns nil, no monitor is attached to the file.
type FileReadMonitorFactory func(name path.Component) FileReadMonitor

// InitialContentsFetcher is called into by PrepopulatedDirectory when a
// directory whose contents need to be instantiated lazily is accessed.
// The results returned by FetchContents() are used to populate the
// directory.
//
// FetchContents() should be called until it succeeds at most once. It
// may be possible FetchContents() is never called. This may happen if
// the directory in question is never accessed.
type InitialContentsFetcher interface {
	InitialNode

	FetchContents(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialChild, error)
}
