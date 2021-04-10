package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type linkHelperClonefile struct{}

var LinkHelperClonefile LinkHelper = linkHelperClonefile{}

func (h linkHelperClonefile) Link(directory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return directory.Clonefile(oldName, newDirectory, newName)
}
