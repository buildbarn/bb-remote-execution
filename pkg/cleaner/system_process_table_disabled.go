//go:build freebsd || windows
// +build freebsd windows

package cleaner

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type systemProcessTable struct{}

func (pt systemProcessTable) GetProcesses() ([]Process, error) {
	return nil, status.Error(codes.Unimplemented, "Scanning the process table is not supported on this platform")
}

// SystemProcessTable corresponds with the process table of the locally
// running operating system. On this operating system this functionality
// is ont available.
var SystemProcessTable ProcessTable = systemProcessTable{}
