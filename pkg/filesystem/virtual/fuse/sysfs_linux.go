//go:build linux
// +build linux

package fuse

import (
	"fmt"
	"os"
	"strconv"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

// SetMaximumDirtyPagesPercentage adjusts the kernel's limit on the
// maximum number of dirty pages belonging to a FUSE mount. The limit is
// specified as a decimal percentage in range [1, 100].
//
// This implementation configures the limit through Linux's sysfs.
func SetMaximumDirtyPagesPercentage(mountPath string, percentage int) error {
	// Construct the path in sysfs of the max_ratio file. The path
	// is based on the major/minor number of the mount's st_dev.
	var sb unix.Stat_t
	if err := unix.Stat(mountPath, &sb); err != nil {
		return util.StatusWrapf(err, "Failed to obtain device number from FUSE mount %#v", mountPath)
	}
	maxRatioPath := fmt.Sprintf("/sys/class/bdi/%d:%d/max_ratio", unix.Major(sb.Dev), unix.Minor(sb.Dev))

	f, err := os.OpenFile(maxRatioPath, os.O_TRUNC|os.O_WRONLY, 0o666)
	if err != nil {
		return util.StatusWrapf(err, "Failed to open %#v corresponding to FUSE mount %#v", maxRatioPath, mountPath)
	}
	_, err1 := f.Write([]byte(strconv.FormatInt(int64(percentage), 10)))
	err2 := f.Close()
	if err1 != nil {
		return util.StatusWrapf(err1, "Failed to write to %#v corresponding to FUSE mount %#v", maxRatioPath, mountPath)
	}
	if err2 != nil {
		return util.StatusWrapf(err2, "Failed to close %#v corresponding to FUSE mount %#v", maxRatioPath, mountPath)
	}
	return nil
}
