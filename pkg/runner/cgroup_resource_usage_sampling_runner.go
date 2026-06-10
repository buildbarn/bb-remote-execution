package runner

import (
	"context"
	"sync/atomic"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// CgroupResourceUsageReader reads cgroup resource usage deltas for a single
// action.
type CgroupResourceUsageReader interface {
	Close() error
	Read() (*resourceusage.CgroupResourceUsage, error)
}

type cgroupResourceUsageSamplingRunner struct {
	runner_pb.RunnerServer
	newCgroupResourceUsageReader func() (CgroupResourceUsageReader, error)
	activeRun                    atomic.Bool
}

// NewCgroupResourceUsageSamplingRunner creates a decorator for RunnerServer
// that samples cgroup v2 resource usage counters around actions and appends
// them to successful Run() responses.
//
// Sampled cgroup counters are only meaningful as per-action deltas if
// bb_worker sends at most one action to the runner at a time
// (RunnerConfiguration.concurrency == 1), and if the runner is deployed in a
// cgroup whose other activity is acceptable to include in the reported usage.
//
// cgroupfsPath is the cgroup v2 filesystem directory whose counters should be
// sampled. A fresh reader is created for each Run() request to capture the
// baseline counters for that action.
func NewCgroupResourceUsageSamplingRunner(base runner_pb.RunnerServer, cgroupfsPath string) runner_pb.RunnerServer {
	return &cgroupResourceUsageSamplingRunner{
		RunnerServer: base,
		newCgroupResourceUsageReader: func() (CgroupResourceUsageReader, error) {
			return NewCgroupResourceUsageReaderFromPath(cgroupfsPath)
		},
	}
}

func (r *cgroupResourceUsageSamplingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if !r.activeRun.CompareAndSwap(false, true) {
		return nil, status.Error(codes.Internal, "cgroup resource usage sampling requires an exclusive runner cgroup, but concurrent Run() calls were observed")
	}
	defer r.activeRun.Store(false)

	cgroupResourceUsageReader, err := r.newCgroupResourceUsageReader()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create cgroup resource usage reader")
	}
	defer func() {
		_ = cgroupResourceUsageReader.Close()
	}()

	response, err := r.RunnerServer.Run(ctx, request)
	if err != nil {
		return response, err
	}

	cgroupUsage, err := cgroupResourceUsageReader.Read()
	if err != nil {
		return response, util.StatusWrap(err, "Failed to read cgroup stats")
	}
	if cgroupUsage == nil {
		return response, nil
	}
	cgroupAny, err := anypb.New(cgroupUsage)
	if err != nil {
		return response, util.StatusWrap(err, "Failed to marshal cgroup resource usage")
	}
	if response != nil {
		response.ResourceUsage = append(response.ResourceUsage, cgroupAny)
	}
	if cgroupUsage.MemoryEventsOomKill > 0 && cgroupUsage.MemoryEventsOom == 0 {
		// The cgroup did not reach its memory limit, so the OOM kill likely
		// came from system-level memory pressure, such as node memory
		// overcommitment. Treat this as retryable infrastructure failure.
		return response, status.Error(codes.Unavailable, "An action process was OOM-killed without the action reaching its cgroup memory limit")
	}
	return response, nil
}
