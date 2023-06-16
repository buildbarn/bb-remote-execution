//go:build darwin || linux
// +build darwin linux

package configuration

import (
	"golang.org/x/sys/unix"
)

// removeStaleMounts cleans up stale FUSE/NFSv4 mounts that were left
// behind by a previous invocation of the program. As FUSE apparently
// allows multiple mounts to be placed on top of a single inode, we must
// call unmount() repeatedly.
func removeStaleMounts(path string) {
	for unix.Unmount(path, 0) == nil {
	}
}
