//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type emptyInitialContentsFetcher struct{}

func (f emptyInitialContentsFetcher) FetchContents() (map[path.Component]InitialNode, error) {
	return map[path.Component]InitialNode{}, nil
}

func (f emptyInitialContentsFetcher) GetContainingDigests(ctx context.Context) (digest.Set, error) {
	return digest.EmptySet, nil
}

// EmptyInitialContentsFetcher is an instance of InitialContentsFetcher
// that yields no children. It can be used in case an empty directory
// needs to be created.
var EmptyInitialContentsFetcher InitialContentsFetcher = emptyInitialContentsFetcher{}
