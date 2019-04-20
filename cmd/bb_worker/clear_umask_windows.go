// +build windows

package main

// Calls syscall.Umask(0) on non-Windows systems.
func clearUmask() {
}
