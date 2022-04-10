//go:build !linux
// +build !linux

package fuse

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetMaximumDirtyPagesPercentage adjusts the kernel's limit on the
// maximum number of dirty pages belonging to a FUSE mount. The limit is
// specified as a decimal percentage in range [1, 100].
//
// This is a placeholder implementation for operating systems that don't
// allow configuring this attribute.
func SetMaximumDirtyPagesPercentage(path string, percentage int) error {
	return status.Error(codes.Unimplemented, "Setting the maximum dirty pages percentage is only supported on Linux")
}
