package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// A LinkHelper implements a filesystem-specific method for copying files.
type LinkHelper interface {
	Link(directory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error
}
