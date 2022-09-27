package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// ReadOnlyDirectory can be embedded into a Directory to disable all
// operations that mutate the directory contents.
type ReadOnlyDirectory struct{}

// VirtualLink is an implementation of the link() system call that
// treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualLink(ctx context.Context, name path.Component, leaf Leaf, requested AttributesMask, out *Attributes) (ChangeInfo, Status) {
	return ChangeInfo{}, StatusErrROFS
}

// VirtualMkdir is an implementation of the mkdir() system call that
// treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualMkdir(name path.Component, requested AttributesMask, out *Attributes) (Directory, ChangeInfo, Status) {
	return nil, ChangeInfo{}, StatusErrROFS
}

// VirtualMknod is an implementation of the mknod() system call that
// treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualMknod(ctx context.Context, name path.Component, fileType filesystem.FileType, requested AttributesMask, out *Attributes) (Leaf, ChangeInfo, Status) {
	return nil, ChangeInfo{}, StatusErrROFS
}

// VirtualRename is an implementation of the rename() system call that
// treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualRename(oldName path.Component, newDirectory Directory, newName path.Component) (ChangeInfo, ChangeInfo, Status) {
	return ChangeInfo{}, ChangeInfo{}, StatusErrROFS
}

// VirtualRemove is an implementation of the unlink() and rmdir() system
// calls that treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualRemove(name path.Component, removeDirectory, removeLeaf bool) (ChangeInfo, Status) {
	return ChangeInfo{}, StatusErrROFS
}

// VirtualSetAttributes is an implementation of the chmod(),
// utimensat(), etc. system calls that treats the target directory as
// being read-only.
func (ReadOnlyDirectory) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	return StatusErrROFS
}

// VirtualSymlink is an implementation of the symlink() system call that
// treats the target directory as being read-only.
func (ReadOnlyDirectory) VirtualSymlink(ctx context.Context, pointedTo []byte, linkName path.Component, requested AttributesMask, out *Attributes) (Leaf, ChangeInfo, Status) {
	return nil, ChangeInfo{}, StatusErrROFS
}

// ReadOnlyDirectoryOpenChildWrongFileType is a helper function for
// implementing Directory.VirtualOpenChild() for read-only directories.
// It can be used to obtain return values in case the directory already
// contains a file under a given name.
func ReadOnlyDirectoryOpenChildWrongFileType(existingOptions *OpenExistingOptions, s Status) (Leaf, AttributesMask, ChangeInfo, Status) {
	if existingOptions == nil {
		return nil, 0, ChangeInfo{}, StatusErrExist
	}
	return nil, 0, ChangeInfo{}, s
}

// ReadOnlyDirectoryOpenChildDoesntExist is a helper function for
// implementing Directory.VirtualOpenChild() for read-only directories.
// It can be used to obtain return values in case the directory doesn't
// contains any file under a given name.
func ReadOnlyDirectoryOpenChildDoesntExist(createAttributes *Attributes) (Leaf, AttributesMask, ChangeInfo, Status) {
	if createAttributes == nil {
		return nil, 0, ChangeInfo{}, StatusErrNoEnt
	}
	return nil, 0, ChangeInfo{}, StatusErrROFS
}
