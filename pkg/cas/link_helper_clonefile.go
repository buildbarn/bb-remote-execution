package cas

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type linkHelperClonefile struct{}

// Provides file copies through the use of clonefile(2).  Only useful on
// darwin with APFS filesystems.  Clonefile makes new inode that looks like
// a complete copy, except the underlying data is shared in copy-on-write
// fashion.
var LinkHelperClonefile LinkHelper = linkHelperClonefile{}

func (h linkHelperClonefile) Link(directory filesystem.Directory, oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return directory.Clonefile(oldName, newDirectory, newName)
}
