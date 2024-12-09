package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type emptyInitialContentsFetcher struct{}

func (f emptyInitialContentsFetcher) FetchContents(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialChild, error) {
	return map[path.Component]InitialChild{}, nil
}

func (f emptyInitialContentsFetcher) VirtualApply(data any) bool {
	return false
}

// EmptyInitialContentsFetcher is an instance of InitialContentsFetcher
// that yields no children. It can be used in case an empty directory
// needs to be created.
var EmptyInitialContentsFetcher InitialContentsFetcher = emptyInitialContentsFetcher{}
