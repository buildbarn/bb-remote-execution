// +build darwin freebsd linux

package cleaner

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func killProcess(id int) error {
	// Ignore EPERM errors, as we may get those if we try to kill
	// setuid processes. Also ignore ESRCH errors, as we can get
	// those if we try to kill a process that already terminated.
	if err := unix.Kill(id, syscall.SIGKILL); err != nil && err != syscall.EPERM && err != syscall.ESRCH {
		return err
	}
	return nil
}
