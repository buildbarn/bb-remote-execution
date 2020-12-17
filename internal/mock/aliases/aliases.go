package aliases

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
)

// This file contains aliases for some of the interfaces provided by the
// FUSE package. These aliases are used to rename them to prevent naming
// collisions with other interface types for which we want to generate
// mocks.

// FUSEDirectory is an alias of fuse.Directory.
type FUSEDirectory = fuse.Directory

// FUSELeaf is an alias of fuse.Leaf.
type FUSELeaf = fuse.Leaf
