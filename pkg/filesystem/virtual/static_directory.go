package virtual

import (
	"context"
	"sort"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type staticDirectoryEntry struct {
	name           path.Component
	normalizedName NormalizedComponent
	child          DirectoryChild
}

type staticDirectoryEntryList []staticDirectoryEntry

func (l staticDirectoryEntryList) Len() int {
	return len(l)
}

func (l staticDirectoryEntryList) Less(i, j int) bool {
	return l[i].normalizedName < l[j].normalizedName
}

func (l staticDirectoryEntryList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type staticDirectory struct {
	ReadOnlyDirectory

	normalizer ComponentNormalizer
	entries    []staticDirectoryEntry
	linkCount  uint32
}

// NewStaticDirectory creates a Directory that contains a hardcoded list
// of child files or directories. The contents of this directory are
// immutable.
func NewStaticDirectory(normalizer ComponentNormalizer, directories map[path.Component]DirectoryChild) Directory {
	// Place all directory entries in a sorted list. This allows us
	// to do lookups by performing a binary search, while also
	// making it possible to implement readdir() deterministically.
	entries := make(staticDirectoryEntryList, 0, len(directories))
	linkCount := EmptyDirectoryLinkCount
	for name, child := range directories {
		entries = append(entries, staticDirectoryEntry{
			name:           name,
			normalizedName: normalizer.Normalize(name),
			child:          child,
		})
		if directory, _ := child.GetPair(); directory != nil {
			linkCount++
		}
	}
	sort.Sort(entries)

	return &staticDirectory{
		normalizer: normalizer,
		entries:    entries,
		linkCount:  linkCount,
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
	normalizedName := d.normalizer.Normalize(name)
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].normalizedName >= normalizedName
	}); i < len(d.entries) && d.entries[i].normalizedName == normalizedName {
		child := d.entries[i].child
		child.GetNode().VirtualGetAttributes(ctx, requested, out)
		return (child), StatusOK
	}
	return DirectoryChild{}, StatusErrNoEnt
}

func (d *staticDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess ShareMask, createAttributes *Attributes, existingOptions *OpenExistingOptions, requested AttributesMask, openedFileAttributes *Attributes) (Leaf, AttributesMask, ChangeInfo, Status) {
	normalizedName := d.normalizer.Normalize(name)
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].normalizedName >= normalizedName
	}); i < len(d.entries) && d.entries[i].normalizedName == normalizedName {
		if existingOptions == nil {
			return nil, 0, ChangeInfo{}, StatusErrExist
		}
		_, leaf := d.entries[i].child.GetPair()
		if leaf == nil {
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

func (staticDirectory) VirtualApply(data any) bool {
	return false
}
