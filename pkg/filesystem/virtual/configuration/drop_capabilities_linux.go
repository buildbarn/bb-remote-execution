//go:build linux
// +build linux

package configuration

import (
	"golang.org/x/sys/unix"
)

// dropMountCapabilities removes capabilities that are needed to mount a FUSE filesystem,
// but not needed after initialization.
func dropMountCapabilities() error {
	return unix.Prctl(unix.PR_CAPBSET_DROP, 21 /* CAP_SYS_ADMIN */, 0, 0, 0)
}
