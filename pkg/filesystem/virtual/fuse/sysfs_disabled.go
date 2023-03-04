//go:build !linux
// +build !linux

package fuse

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetLinuxBackingDevInfoTunables adjusts tunables of the Backing Dev
// Info (BDI) belonging to a FUSE mount. These tunables can, for
// example, be used to increase the maximum number of dirty pages
// belonging to the mount.
//
// This is a placeholder implementation for operating systems other than
// Linux.
func SetLinuxBackingDevInfoTunables(mountPath string, variables map[string]string) error {
	if len(variables) > 0 {
		return status.Error(codes.Unimplemented, "Setting Linux Backing Dev Info tunables is only supported on Linux")
	}
	return nil
}
