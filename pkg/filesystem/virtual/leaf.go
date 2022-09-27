package virtual

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// ShareMask is a bitmask of operations that are permitted against a
// Leaf that has been opened.
type ShareMask uint32

const (
	// ShareMaskRead permits calls to VirtualRead().
	ShareMaskRead ShareMask = 1 << iota
	// ShareMaskWrite permits calls to VirtualWrite().
	ShareMaskWrite
)

// OpenExistingOptions contains options that describe what should happen
// with a file when opened. The Truncate option corresponds to open()'s
// O_TRUNC option. This option has no effect on freshly created files,
// as those are always empty.
type OpenExistingOptions struct {
	Truncate bool
}

// ToAttributesMask converts open options to an AttributeMask,
// indicating which file attributes were affected by the operation.
func (o *OpenExistingOptions) ToAttributesMask() (m AttributesMask) {
	if o.Truncate {
		m |= AttributesMaskSizeBytes
	}
	return
}

// Leaf node that is exposed through FUSE using SimpleRawFileSystem, or
// through NFSv4. Examples of leaf nodes are regular files, sockets,
// FIFOs, symbolic links and devices.
//
// TODO: Should all methods take an instance of Context?
type Leaf interface {
	Node

	VirtualAllocate(off, size uint64) Status
	VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status)
	VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status
	VirtualRead(buf []byte, offset uint64) (n int, eof bool, s Status)
	VirtualReadlink(ctx context.Context) ([]byte, Status)
	VirtualClose(count uint)
	VirtualWrite(buf []byte, offset uint64) (int, Status)
}

// StatelessLeafLinkCount is the value that should be assigned to
// fuse.Attr.Nlink for leaf nodes that don't track an explicit link
// count, such as files backed by the Content Addressable Storage (CAS).
//
// The Linux kernel doesn't treat fuse.Attr.Nlink as an opaque value.
// Its value gets stored in the kernel inode structure's i_nlink field.
// This cached value may later be incremented and decremented inside
// fuse_link() and fuse_unlink(), meaning that it may reach zero. Once
// zero, the kernel thinks the file is unlinked, causing future link()
// calls to fail.
//
// This means that the value of fuse.Attr.Nlink should ideally reflect
// the true number of paths under which these files are visible. For
// stateless files that is impossible to achieve, as they may appear in
// an arbitrary number of places that aren't known up front. Solve this
// by using a constant value that is sufficiently high for most use
// cases.
//
// References:
// - https://github.com/torvalds/linux/blob/01c70267053d6718820ac0902d8823d5dd2a6adb/fs/fuse/inode.c#L161
// - https://github.com/torvalds/linux/blob/01c70267053d6718820ac0902d8823d5dd2a6adb/fs/fuse/dir.c#L874
// - https://github.com/torvalds/linux/blob/01c70267053d6718820ac0902d8823d5dd2a6adb/fs/fuse/dir.c#L726
// - https://github.com/torvalds/linux/blob/01c70267053d6718820ac0902d8823d5dd2a6adb/fs/namei.c#L4066-L4067
const StatelessLeafLinkCount = 9999

// BoundReadToFileSize is a helper function for implementations of
// VirtualRead() to limit the read size to the actual file size.
func BoundReadToFileSize(buf []byte, offset, size uint64) ([]byte, bool) {
	if offset >= size {
		// Read starting at or past end-of-file.
		return nil, true
	}
	if remaining := size - offset; uint64(len(buf)) >= remaining {
		// Read ending at or past end-of-file.
		return buf[:remaining], true
	}
	return buf, false
}
