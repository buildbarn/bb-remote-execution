package virtual

import (
	"context"
	"sort"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type staticDirectoryEntry struct {
	name  path.Component
	child DirectoryChild
}

type staticDirectoryEntryList []staticDirectoryEntry

func (l staticDirectoryEntryList) Len() int {
	return len(l)
}

func (l staticDirectoryEntryList) Less(i, j int) bool {
	return l[i].name.String() < l[j].name.String()
}

func (l staticDirectoryEntryList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type staticDirectory struct {
	ReadOnlyDirectory

	entries   []staticDirectoryEntry
	linkCount uint32
}

// NewStaticDirectory creates a Directory that contains a hardcoded list
// of child files or directories. The contents of this directory are
// immutable.
func NewStaticDirectory(directories map[path.Component]DirectoryChild) Directory {
	// Place all directory entries in a sorted list. This allows us
	// to do lookups by performing a binary search, while also
	// making it possible to implement readdir() deterministically.
	entries := make(staticDirectoryEntryList, 0, len(directories))
	linkCount := EmptyDirectoryLinkCount
	for name, child := range directories {
		entries = append(entries, staticDirectoryEntry{
			name:  name,
			child: child,
		})
		if directory, _ := child.GetPair(); directory != nil {
			linkCount++
		}
	}
	sort.Sort(entries)

	return &staticDirectory{
		entries:   entries,
		linkCount: linkCount,
	}
}

func (d *staticDirectory) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetLinkCount(d.linkCount)
	attributes.SetPermissions(PermissionsRead | PermissionsExecute)
	attributes.SetSizeBytes(0)
}

func (d *staticDirectory) VirtualLookup(ctx context.Context, name path.Component, requested AttributesMask, out *Attributes) (DirectoryChild, Status) {
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].name.String() >= name.String()
	}); i < len(d.entries) && d.entries[i].name == name {
		child := d.entries[i].child
		child.GetNode().VirtualGetAttributes(ctx, requested, out)
		return (child), StatusOK
	}
	return DirectoryChild{}, StatusErrNoEnt
}

func (d *staticDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess ShareMask, createAttributes *Attributes, existingOptions *OpenExistingOptions, requested AttributesMask, openedFileAttributes *Attributes) (Leaf, AttributesMask, ChangeInfo, Status) {
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].name.String() >= name.String()
	}); i < len(d.entries) && d.entries[i].name == name {
		if existingOptions == nil {
			return nil, 0, ChangeInfo{}, StatusErrExist
		}
		directory, leaf := d.entries[i].child.GetPair()
		if directory != nil {
			return nil, 0, ChangeInfo{}, StatusErrIsDir
		}
		s := leaf.VirtualOpenSelf(ctx, shareAccess, existingOptions, requested, openedFileAttributes)
		return leaf, existingOptions.ToAttributesMask(), ChangeInfo{
			Before: 0,
			After:  0,
		}, s
	}
	return ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
}

func (d *staticDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested AttributesMask, reporter DirectoryEntryReporter) Status {
	for i := firstCookie; i < uint64(len(d.entries)); i++ {
		entry := d.entries[i]
		var attributes Attributes
		entry.child.GetNode().VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(i+1, entry.name, entry.child, &attributes) {
			break
		}
	}
	return StatusOK
}
