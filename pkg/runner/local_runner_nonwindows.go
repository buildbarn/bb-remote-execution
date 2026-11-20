//go:build !windows
// +build !windows

package runner

import (
	"syscall"
)

// sysProcAttrForArguments is only meaningful on Windows, where the
// command line of a process is a single string instead of a list of
// arguments.
func sysProcAttrForArguments(sysProcAttr *syscall.SysProcAttr, arguments []string) *syscall.SysProcAttr {
	return sysProcAttr
}
