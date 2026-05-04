package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCgroupResourceUsageSamplingRunnerAppendsResourceUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupStatsReader := &fixedCgroupStatsReader{
		usage: &resourceusage.CgroupResourceUsage{
			MemoryEventsOomKill: 1,
			MemoryPeakBytes:     4096,
			PsiCpuSome:          durationpb.New(123 * time.Microsecond),
			PsiCpuFull:          durationpb.New(45 * time.Microsecond),
		},
	}
	wrappedRunner := newCgroupResourceUsageSamplingRunner(baseRunner, func() (scopedCgroupStatsReader, error) {
		return cgroupStatsReader, nil
	})

	request := &runner_pb.RunRequest{}
	baseRunner.EXPECT().Run(gomock.Any(), request).Return(&runner_pb.RunResponse{
		ExitCode: 7,
	}, nil)

	response, err := wrappedRunner.Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, int64(7), response.ExitCode)
	require.Len(t, response.ResourceUsage, 1)
	var got resourceusage.CgroupResourceUsage
	require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&got))
	require.Equal(t, cgroupStatsReader.usage.GetMemoryEventsOomKill(), got.GetMemoryEventsOomKill())
	require.Equal(t, cgroupStatsReader.usage.GetMemoryPeakBytes(), got.GetMemoryPeakBytes())
	require.Equal(t, cgroupStatsReader.usage.GetPsiCpuSome().AsDuration(), got.GetPsiCpuSome().AsDuration())
	require.Equal(t, cgroupStatsReader.usage.GetPsiCpuFull().AsDuration(), got.GetPsiCpuFull().AsDuration())
	require.True(t, cgroupStatsReader.closed)
}

func TestCgroupResourceUsageSamplingRunnerReadErrorPreservesRunResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupStatsReader := &fixedCgroupStatsReader{
		err: errors.New("read failed"),
	}
	wrappedRunner := newCgroupResourceUsageSamplingRunner(baseRunner, func() (scopedCgroupStatsReader, error) {
		return cgroupStatsReader, nil
	})

	request := &runner_pb.RunRequest{}
	baseResponse := &runner_pb.RunResponse{ExitCode: 7}
	baseRunner.EXPECT().Run(gomock.Any(), request).Return(baseResponse, nil)

	response, err := wrappedRunner.Run(context.Background(), request)
	require.NoError(t, err)
	require.Same(t, baseResponse, response)
	require.Empty(t, response.ResourceUsage)
	require.True(t, cgroupStatsReader.closed)
}

func TestCgroupResourceUsageSamplingRunnerRunErrorDoesNotReadCgroupUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	cgroupStatsReader := &fixedCgroupStatsReader{
		usage: &resourceusage.CgroupResourceUsage{
			MemoryPeakBytes: 4096,
		},
	}
	wrappedRunner := newCgroupResourceUsageSamplingRunner(baseRunner, func() (scopedCgroupStatsReader, error) {
		return cgroupStatsReader, nil
	})

	baseErr := status.Error(codes.FailedPrecondition, "failed")
	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).Return(nil, baseErr)

	response, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
	require.Nil(t, response)
	require.Equal(t, baseErr, err)
	require.False(t, cgroupStatsReader.read)
	require.True(t, cgroupStatsReader.closed)
}

func TestCgroupResourceUsageSamplingRunnerRejectsConcurrentRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRunner := mock.NewMockRunnerServer(ctrl)
	wrappedRunner := newCgroupResourceUsageSamplingRunner(baseRunner, func() (scopedCgroupStatsReader, error) {
		return noopCgroupStatsReader{}, nil
	})

	started := make(chan struct{})
	release := make(chan struct{})
	baseRunner.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			close(started)
			<-release
			return &runner_pb.RunResponse{}, nil
		})

	firstRunErr := make(chan error, 1)
	go func() {
		_, err := wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
		firstRunErr <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Run() to enter wrapped runner")
	}

	secondRunDone := make(chan struct{})
	var response *runner_pb.RunResponse
	var err error
	go func() {
		response, err = wrappedRunner.Run(context.Background(), &runner_pb.RunRequest{})
		close(secondRunDone)
	}()
	select {
	case <-secondRunDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for concurrent Run() rejection")
	}
	require.Nil(t, response)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "concurrent Run() calls")

	close(release)
	select {
	case err := <-firstRunErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Run() to finish")
	}
}

type fixedCgroupStatsReader struct {
	usage  *resourceusage.CgroupResourceUsage
	err    error
	read   bool
	closed bool
}

func (r *fixedCgroupStatsReader) Close() error {
	r.closed = true
	return nil
}

func (r *fixedCgroupStatsReader) Read() (*resourceusage.CgroupResourceUsage, error) {
	r.read = true
	return r.usage, r.err
}

type noopCgroupStatsReader struct{}

func (noopCgroupStatsReader) Close() error {
	return nil
}

func (noopCgroupStatsReader) Read() (*resourceusage.CgroupResourceUsage, error) {
	return nil, nil
}
