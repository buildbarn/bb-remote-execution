//go:build !linux

package runner

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResolveCurrentCgroupfsPath returns an error, as cgroup resource usage
// sampling is only supported on Linux.
func ResolveCurrentCgroupfsPath() (string, error) {
	return "", status.Error(codes.Unimplemented, "cgroup resource usage sampling is only supported on Linux")
}

// ResolveCurrentCgroupfsPathFromProcFiles returns an error, as cgroup resource
// usage sampling is only supported on Linux.
func ResolveCurrentCgroupfsPathFromProcFiles(procCgroupPath, procMountInfoPath string) (string, error) {
	return "", status.Error(codes.Unimplemented, "cgroup resource usage sampling is only supported on Linux")
}

// NewCgroupResourceUsageReaderFromPath returns an error, as cgroup resource
// usage sampling is only supported on Linux.
func NewCgroupResourceUsageReaderFromPath(cgroupPath string) (CgroupResourceUsageReader, error) {
	return nil, status.Error(codes.Unimplemented, "cgroup resource usage sampling is only supported on Linux")
}
