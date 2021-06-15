// +build darwin linux

package fuse

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Leaf node that is exposed through FUSE using SimpleRawFileSystem.
// Examples of leaf nodes are regular files, sockets, FIFOs, symbolic
// links and devices.
type Leaf interface {
	node

	FUSEFallocate(off, size uint64) fuse.Status
	FUSELseek(in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status
	FUSEOpen(flags uint32) fuse.Status
	FUSERead(buf []byte, offset uint64) (fuse.ReadResult, fuse.Status)
	FUSEReadlink() ([]byte, fuse.Status)
	FUSERelease()
	FUSEWrite(buf []byte, offset uint64) (uint32, fuse.Status)
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
