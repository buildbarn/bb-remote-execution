//go:build linux
// +build linux

package fuse

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

// SetLinuxBackingDevInfoTunables adjusts tunables of the Linux Backing
// Dev Info (BDI) belonging to a FUSE mount. These tunables can, for
// example, be used to increase the maximum number of dirty pages
// belonging to the mount.
//
// This implementation applies the tunables through Linux's sysfs.
func SetLinuxBackingDevInfoTunables(mountPath string, variables map[string]string) error {
	// Construct the path in sysfs of the max_ratio file. The path
	// is based on the major/minor number of the mount's st_dev.
	var sb unix.Stat_t
	if err := unix.Stat(mountPath, &sb); err != nil {
		return util.StatusWrapf(err, "Failed to obtain device number from FUSE mount %#v", mountPath)
	}
	bdiPath := fmt.Sprintf("/sys/class/bdi/%d:%d", unix.Major(sb.Dev), unix.Minor(sb.Dev))

	for key, value := range variables {
		keyPath := filepath.Join(bdiPath, key)
		f, err := os.OpenFile(keyPath, os.O_TRUNC|os.O_WRONLY, 0o666)
		if err != nil {
			return util.StatusWrapf(err, "Failed to open %#v corresponding to FUSE mount %#v", keyPath, mountPath)
		}
		_, err1 := f.Write([]byte(value))
		err2 := f.Close()
		if err1 != nil {
			return util.StatusWrapf(err1, "Failed to write to %#v corresponding to FUSE mount %#v", keyPath, mountPath)
		}
		if err2 != nil {
			return util.StatusWrapf(err2, "Failed to close %#v corresponding to FUSE mount %#v", keyPath, mountPath)
		}
	}
	return nil
}
