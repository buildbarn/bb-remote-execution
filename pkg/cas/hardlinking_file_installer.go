package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type hardlinkingFileInstaller struct{}

// HardlinkingFileInstaller provides file copies through the use of link(2)
var HardlinkingFileInstaller FileInstaller = hardlinkingFileInstaller{}

func (h hardlinkingFileInstaller) Link(oldDirectory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return oldDirectory.Link(oldName, newDirectory, newName)
}
