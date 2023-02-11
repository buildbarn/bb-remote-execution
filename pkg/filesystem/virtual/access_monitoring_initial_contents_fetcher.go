package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type accessMonitoringInitialContentsFetcher struct {
	InitialContentsFetcher
	unreadDirectoryMonitor access.UnreadDirectoryMonitor
}

// NewAccessMonitoringInitialContentsFetcher decorates an
// InitialContentsFetcher, so that any read access to files and
// directories is reported to an UnreadDirectoryMonitor. This can be
// used to create file system access profiles of build actions.
func NewAccessMonitoringInitialContentsFetcher(base InitialContentsFetcher, rootDirectoryMonitor access.UnreadDirectoryMonitor) InitialContentsFetcher {
	return &accessMonitoringInitialContentsFetcher{
		InitialContentsFetcher: base,
		unreadDirectoryMonitor: rootDirectoryMonitor,
	}
}

func (icf *accessMonitoringInitialContentsFetcher) FetchContents(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialNode, error) {
	// Call into underlying initial contents fetcher. Wrap the file
	// read monitors that are installed on the files, so that we can
	// detect file access.
	readDirectoryMonitor := icf.unreadDirectoryMonitor.ReadDirectory()
	contents, err := icf.InitialContentsFetcher.FetchContents(func(name path.Component) FileReadMonitor {
		if fileReadMonitor := fileReadMonitorFactory(name); fileReadMonitor != nil {
			return func() {
				fileReadMonitor()
				readDirectoryMonitor.ReadFile(name)
			}
		}
		return func() {
			readDirectoryMonitor.ReadFile(name)
		}
	})
	if err != nil {
		return nil, err
	}

	// Wrap all of the child directories, so that we can detect
	// directory access.
	wrappedContents := make(map[path.Component]InitialNode, len(contents))
	for name, node := range contents {
		childInitialContentsFetcher, leaf := node.GetPair()
		if childInitialContentsFetcher != nil {
			wrappedContents[name] = InitialNode{}.FromDirectory(&accessMonitoringInitialContentsFetcher{
				InitialContentsFetcher: childInitialContentsFetcher,
				unreadDirectoryMonitor: readDirectoryMonitor.ResolvedDirectory(name),
			})
		} else {
			wrappedContents[name] = InitialNode{}.FromLeaf(leaf)
		}
	}
	return wrappedContents, nil
}
