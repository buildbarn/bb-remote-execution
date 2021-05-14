package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type cloningFileInstaller struct{}

// CloningFileInstaller provides file copies through the use of clonefile(2).
// Only useful on darwin with APFS filesystems.  Clonefile makes new inode that
// looks like a complete copy, except the underlying data is shared in
// copy-on-write fashion.
var CloningFileInstaller FileInstaller = cloningFileInstaller{}

func (h cloningFileInstaller) Link(oldDirectory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return oldDirectory.Clonefile(oldName, newDirectory, newName)
}
