package runner

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type cgroupResourceUsageSamplingRunner struct {
	runner_pb.RunnerServer
	newScopedCgroupStatsReader scopedCgroupStatsReaderFactory
	activeRuns                 atomic.Int32
}

type scopedCgroupStatsReader interface {
	Close() error
	Read() (*resourceusage.CgroupResourceUsage, error)
}

type scopedCgroupStatsReaderFactory func() (scopedCgroupStatsReader, error)

func newDefaultScopedCgroupStatsReader() (scopedCgroupStatsReader, error) {
	return newScopedCgroupStatsReader()
}

// NewCgroupResourceUsageSamplingRunner creates a decorator for RunnerServer
// that samples cgroup v2 resource usage counters around actions and appends
// them to successful Run() responses.
//
// This decorator requires the runner's cgroup to be exclusive, so that
// sampled cgroup counters can be interpreted as per-action deltas.
func NewCgroupResourceUsageSamplingRunner(base runner_pb.RunnerServer) (runner_pb.RunnerServer, error) {
	if err := validateExclusiveCgroupResourceUsageSampling(); err != nil {
		return nil, err
	}
	return newCgroupResourceUsageSamplingRunner(base, newDefaultScopedCgroupStatsReader), nil
}

func newCgroupResourceUsageSamplingRunner(base runner_pb.RunnerServer, newScopedCgroupStatsReader scopedCgroupStatsReaderFactory) runner_pb.RunnerServer {
	return &cgroupResourceUsageSamplingRunner{
		RunnerServer:               base,
		newScopedCgroupStatsReader: newScopedCgroupStatsReader,
	}
}

func (r *cgroupResourceUsageSamplingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if r.activeRuns.Add(1) != 1 {
		r.activeRuns.Add(-1)
		return nil, status.Error(codes.Internal, "cgroup resource usage sampling requires an exclusive runner cgroup, but concurrent Run() calls were observed")
	}
	defer r.activeRuns.Add(-1)

	cgroupStatsReader, err := r.newScopedCgroupStatsReader()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create scoped cgroup stats reader: %s", err)
	}
	defer func() {
		_ = cgroupStatsReader.Close()
	}()

	response, err := r.RunnerServer.Run(ctx, request)
	if err != nil {
		return response, err
	}

	cgroupUsage, readErr := cgroupStatsReader.Read()
	if readErr != nil {
		log.Print("Failed to read scoped cgroup stats: ", readErr)
		return response, err
	}
	if cgroupUsage == nil {
		return response, err
	}
	cgroupAny, cgroupErr := anypb.New(cgroupUsage)
	if cgroupErr != nil {
		return response, err
	}
	if response != nil {
		response.ResourceUsage = append(response.ResourceUsage, cgroupAny)
	}
	return response, nil
}
