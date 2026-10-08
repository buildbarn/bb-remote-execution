//go:build windows

package cas

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isHardlinkLimitReached reports whether err is the filesystem's "too many hard
// links" error. NTFS returns ERROR_TOO_MANY_LINKS after 1023 links to a file.
func isHardlinkLimitReached(err error) bool {
	return errors.Is(err, windows.ERROR_TOO_MANY_LINKS)
}
