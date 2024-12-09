package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type emptyInitialContentsFetcher struct{}

func (f emptyInitialContentsFetcher) FetchContents(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialChild, error) {
	return map[path.Component]InitialChild{}, nil
}

func (f emptyInitialContentsFetcher) GetContainingDigests(ctx context.Context) (digest.Set, error) {
	return digest.EmptySet, nil
}

// EmptyInitialContentsFetcher is an instance of InitialContentsFetcher
// that yields no children. It can be used in case an empty directory
// needs to be created.
var EmptyInitialContentsFetcher InitialContentsFetcher = emptyInitialContentsFetcher{}
