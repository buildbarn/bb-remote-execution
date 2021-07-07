package aliases

import (
	"context"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
)

// This file contains aliases for some of the interfaces provided by the
// Go standard library. The only reason this file exists is to allow the
// gomock() Bazel rule to emit mocks for them, as that rule is only
// capable of emitting mocks for interfaces built through a
// go_library().
//
// It also contains aliases for some of the interfaces provided by the
// FUSE package. These aliases are used to rename them to prevent naming
// collisions with other interface types for which we want to generate
// mocks.

// CancelFunc is an alias of context.CancelFunc.
type CancelFunc = context.CancelFunc

// Context is an alias of context.Context.
type Context = context.Context

// FUSEDirectory is an alias of fuse.Directory.
type FUSEDirectory = fuse.Directory

// FUSELeaf is an alias of fuse.Leaf.
type FUSELeaf = fuse.Leaf

// ReadCloser is an alias of io.ReadCloser.
type ReadCloser = io.ReadCloser
