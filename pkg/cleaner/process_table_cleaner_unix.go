// +build darwin freebsd linux

package cleaner

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func killProcess(id int) error {
	if err := unix.Kill(id, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
