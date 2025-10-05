package virtual

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type inMemoryNamedAttributesFactory struct {
	subtree *inMemorySubtree
}

// NewInMemoryNamedAttributesFactory creates a factory for
// NamedAttributes objects. The named attribute directory that is used
// by these instances is backed by an ordinary in-memory directory,
// meaning that the metadata of the named attributes is kept in memory.
func NewInMemoryNamedAttributesFactory(fileAllocator FileAllocator, symlinkFactory SymlinkFactory, errorLogger util.ErrorLogger, handleAllocator StatefulHandleAllocator, clock clock.Clock) NamedAttributesFactory {
	return &inMemoryNamedAttributesFactory{
		subtree: newInMemorySubtree(
			fileAllocator,
			symlinkFactory,
			errorLogger,
			handleAllocator,
			sort.Sort,
			/* hiddenFilesPattern = */ func(s string) bool { return false },
			clock,
			CaseSensitiveComponentNormalizer,
			/* defaultAttributesSetter = */ func(requested AttributesMask, attributes *Attributes) {},
			InNamedAttributeDirectoryNamedAttributesFactory,
		),
	}
}

func (naf *inMemoryNamedAttributesFactory) NewNamedAttributes() NamedAttributes {
	return &inMemoryNamedAttributes{
		subtree: naf.subtree,
	}
}

type inMemoryNamedAttributes struct {
	subtree   *inMemorySubtree
	directory atomic.Pointer[inMemoryPrepopulatedDirectory]
}

func (na *inMemoryNamedAttributes) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	if requested&AttributesMaskHasNamedAttributes != 0 {
		hasNamedAttributes := false
		if existingDirectory := na.directory.Load(); existingDirectory != nil {
			// This node has a named attribute directory,
			// meaning that OPENATTR was previously called
			// against this node. Check whether the named
			// attribute directory is non-empty.
			existingDirectory.lock.Lock()
			hasNamedAttributes = existingDirectory.contents.entriesList.next != &existingDirectory.contents.entriesList
			existingDirectory.lock.Unlock()
		}
		attributes.SetHasNamedAttributes(hasNamedAttributes)
	}
	attributes.SetIsInNamedAttributeDirectory(false)
}

func (na *inMemoryNamedAttributes) VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested AttributesMask, attributes *Attributes) (Directory, Status) {
	for {
		if existingDirectory := na.directory.Load(); existingDirectory != nil {
			// This node already has a named attribute directory.
			existingDirectory.VirtualGetAttributes(ctx, requested, attributes)
			return existingDirectory, StatusOK
		}
		if !createDirectory {
			return nil, StatusErrNoEnt
		}

		// Create a new named attribute directory and attach it.
		newDirectory := na.subtree.createNewDirectory(EmptyInitialContentsFetcher)
		if na.directory.CompareAndSwap(nil, newDirectory) {
			newDirectory.VirtualGetAttributes(ctx, requested, attributes)
			return newDirectory, StatusOK
		}
		newDirectory.removeAllChildren(true)
	}
}

func (na *inMemoryNamedAttributes) Release() {
	if existingDirectory := na.directory.Load(); existingDirectory != nil {
		existingDirectory.removeAllChildren(true)
	}
}
