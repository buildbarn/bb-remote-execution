//go:build !linux
// +build !linux

package runner

import (
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewCgroupResourceUsageSamplingRunner returns an error, as cgroup resource
// usage sampling is only supported on Linux.
func NewCgroupResourceUsageSamplingRunner(base runner_pb.RunnerServer) (runner_pb.RunnerServer, error) {
	return nil, status.Error(codes.Unimplemented, "cgroup resource usage sampling is only supported on Linux")
}
