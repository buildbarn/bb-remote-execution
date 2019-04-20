// +build !windows

package main

import (
	"syscall"
)

// Calls syscall.Umask(0) on non-Windows systems.
func clearUmask() {
	syscall.Umask(0)
}
