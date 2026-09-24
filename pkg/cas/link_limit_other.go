//go:build !windows

package cas

import (
	"errors"
	"syscall"
)

// isHardlinkLimitReached reports whether err is the filesystem's "too many hard
// links" error (EMLINK on POSIX).
func isHardlinkLimitReached(err error) bool {
	return errors.Is(err, syscall.EMLINK)
}
