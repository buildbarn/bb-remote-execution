package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// Node is the intersection between Directory and Leaf. These are the
// operations that can be applied to both kinds of objects.
type Node interface {
	VirtualGetAttributes(requested AttributesMask, attributes *Attributes)
	VirtualSetAttributes(in *Attributes, requested AttributesMask, attributes *Attributes) Status
}

// GetFileInfo extracts the attributes of a node and returns it in the
// form of a FileInfo object.
func GetFileInfo(name path.Component, node Node) filesystem.FileInfo {
	var attributes Attributes
	node.VirtualGetAttributes(AttributesMaskFileType|AttributesMaskPermissions, &attributes)
	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Node did not return permissions attribute, even though it was requested")
	}
	return filesystem.NewFileInfo(
		name,
		attributes.GetFileType(),
		permissions&PermissionsExecute != 0)
}
