package virtual

import (
	"context"
)

// NamedAttributesFactory is used by types like PoolBackedFileAllocator
// and InMemoryPrepopulatedDirectory to create NamedAttributes objects
// that are embedded into their respective file and directory
// implementations.
type NamedAttributesFactory interface {
	NewNamedAttributes() NamedAttributes
}

// NamedAttributes is embedded in file and directory implementations
// whenever they need to keep track of NFSv4 named attributes. Named
// attributes can be used to store extended file attributes, or resource
// forks. In the case of NFSv4, named attributes are modeled as a
// special kind of directory, where each attribute is stored in a
// separate file.
type NamedAttributes interface {
	VirtualGetAttributes(requested AttributesMask, attributes *Attributes)
	VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested AttributesMask, attributes *Attributes) (Directory, Status)
	Release()
}

type inNamedAttributeDirectoryNamedAttributesFactory struct{}

// InNamedAttributeDirectoryNamedAttributesFactory is a instance of
// NamedAttributesFactory that creates a NamedAttributes object that
// should be used by files and directories that are already part of a
// named attribute directory. The purpose of this type is twofold:
//
//   - To ensure that the NFSv4 server returns special file types
//     NF4NAMEDATTR and NF4ATTRDIR instead of NF4REG and NF4DIR for
//     nodes belonging to named attribute directories.
//
//   - To ensure that nodes belonging to named attribute directories
//     cannot have named attributes themselves.
var InNamedAttributeDirectoryNamedAttributesFactory NamedAttributesFactory = inNamedAttributeDirectoryNamedAttributesFactory{}

func (inNamedAttributeDirectoryNamedAttributesFactory) NewNamedAttributes() NamedAttributes {
	return inNamedAttributeDirectoryNamedAttributes{}
}

type inNamedAttributeDirectoryNamedAttributes struct{}

func (inNamedAttributeDirectoryNamedAttributes) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	attributes.SetHasNamedAttributes(false)
	attributes.SetIsInNamedAttributeDirectory(true)
}

func (inNamedAttributeDirectoryNamedAttributes) VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested AttributesMask, attributes *Attributes) (Directory, Status) {
	// Attempting to call OPENATTR against a named attribute or
	// named attribute directory should fail with
	// NFS4ERR_WRONG_TYPE.
	return nil, StatusErrWrongType
}

func (inNamedAttributeDirectoryNamedAttributes) Release() {}

type noNamedAttributesFactory struct{}

// NoNamedAttributesFactory is an instance of NamedAttributesFactory
// that creates a NamedAttributes object that does not permit storing
// named attributes. This can be used by files and directories that are
// intended to be immutable.
var NoNamedAttributesFactory NamedAttributesFactory = noNamedAttributesFactory{}

func (noNamedAttributesFactory) NewNamedAttributes() NamedAttributes {
	return noNamedAttributes{}
}

type noNamedAttributes struct{}

func (noNamedAttributes) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	attributes.SetHasNamedAttributes(false)
	attributes.SetIsInNamedAttributeDirectory(false)
}

func (noNamedAttributes) VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested AttributesMask, attributes *Attributes) (Directory, Status) {
	if createDirectory {
		return nil, StatusErrAccess
	}
	return nil, StatusErrNoEnt
}

func (noNamedAttributes) Release() {}
