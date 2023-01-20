package virtual

import (
	"sync"

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

func (icf *accessMonitoringInitialContentsFetcher) FetchContents() (map[path.Component]InitialNode, error) {
	// Call into underlying initial contents fetcher.
	contents, err := icf.InitialContentsFetcher.FetchContents()
	if err != nil {
		return nil, err
	}

	// Wrap all of the child files and directories.
	readDirectoryMonitor := icf.unreadDirectoryMonitor.ReadDirectory()
	wrappedContents := make(map[path.Component]InitialNode, len(contents))
	for name, node := range contents {
		childInitialContentsFetcher, leaf := node.GetPair()
		if childInitialContentsFetcher != nil {
			wrappedContents[name] = InitialNode{}.FromDirectory(&accessMonitoringInitialContentsFetcher{
				InitialContentsFetcher: childInitialContentsFetcher,
				unreadDirectoryMonitor: readDirectoryMonitor.ResolvedDirectory(name),
			})
		} else {
			wrappedContents[name] = InitialNode{}.FromLeaf(&accessMonitoringNativeLeaf{
				NativeLeaf:           leaf,
				readDirectoryMonitor: readDirectoryMonitor,
				name:                 name,
			})
		}
	}
	return wrappedContents, nil
}

// accessMonitoringNativeLeaf is a decorator for NativeLeaf that reports
// read operations against files to a ReadDirectoryMonitor.
type accessMonitoringNativeLeaf struct {
	NativeLeaf

	once                 sync.Once
	readDirectoryMonitor access.ReadDirectoryMonitor
	name                 path.Component
}

func (l *accessMonitoringNativeLeaf) reportRead() {
	l.readDirectoryMonitor.ReadFile(l.name)
	l.readDirectoryMonitor = nil
	l.name = path.Component{}
}

func (l *accessMonitoringNativeLeaf) VirtualRead(buf []byte, off uint64) (int, bool, Status) {
	l.once.Do(l.reportRead)
	return l.NativeLeaf.VirtualRead(buf, off)
}
