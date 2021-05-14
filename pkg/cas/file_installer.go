package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// A FileInstaller implements a filesystem-specific method for copying files.
type FileInstaller interface {
	Link(directory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error
}
