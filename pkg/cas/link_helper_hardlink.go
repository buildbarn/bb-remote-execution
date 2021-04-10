package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type linkHelperHardlink struct{}

var LinkHelperHardlink LinkHelper = linkHelperHardlink{}

func (h linkHelperHardlink) Link(directory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return directory.Link(oldName, newDirectory, newName)
}
