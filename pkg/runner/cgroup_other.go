//go:build !linux

package runner

import (
	"fmt"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
)

type cgroupStatsReader struct{}

func newScopedCgroupStatsReader() (*cgroupStatsReader, error) {
	return &cgroupStatsReader{}, nil
}

func (r *cgroupStatsReader) Close() error { return nil }

func (r *cgroupStatsReader) Read() (*resourceusage.CgroupResourceUsage, error) {
	return nil, nil
}

func validateExclusiveCgroupResourceUsageSampling() error {
	return fmt.Errorf("cgroup resource usage sampling is only supported on Linux")
}
