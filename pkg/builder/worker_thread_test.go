package builder_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkerThreadShutdown(test *testing.T) {
	for _, mode := range []string{"Cleanup", "CleanupFailure", "NoCleanup"} {
		test.Run(mode, func(test *testing.T) {
			ctrl := gomock.NewController(test)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			scheduler := mock.NewMockOperationQueueClient(ctrl)
			executor := mock.NewMockBuildExecutor(ctrl)
			client := builder.NewBuildClient(scheduler, executor, pool.EmptyFilePool, clock.SystemClock, nil, digest.EmptyInstanceName, nil, 0)
			executor.EXPECT().CheckReadiness(gomock.Any()).Return(nil)
			scheduler.EXPECT().Synchronize(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *remoteworker.SynchronizeRequest, options ...grpc.CallOption) (*remoteworker.SynchronizeResponse, error) {
				cancel()
				return &remoteworker.SynchronizeResponse{
					NextSynchronizationAt: timestamppb.New(time.Unix(0, 0)),
					DesiredState: &remoteworker.DesiredState{WorkerState: &remoteworker.DesiredState_Executing_{
						Executing: &remoteworker.DesiredState_Executing{DigestFunction: remoteexecution.DigestFunction_SHA256},
					}},
				}, nil
			})
			executionFinished := make(chan struct{})
			executor.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, function digest.Function, request *remoteworker.DesiredState_Executing, updates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
				<-ctx.Done()
				for range 25 {
					updates <- &remoteworker.CurrentState_Executing{}
				}
				close(executionFinished)
				return builder.NewDefaultExecuteResponse(request)
			})
			cleanupFinished := make(chan struct{})
			dependencyStopped := make(chan struct{})
			var cleanup func() error
			if mode != "NoCleanup" {
				cleanup = func() error {
					select {
					case <-executionFinished:
					default:
						test.Error("Execution was not joined before cleanup")
					}
					select {
					case <-dependencyStopped:
						test.Error("Dependency stopped before cleanup")
					default:
					}
					close(cleanupFinished)
					if mode == "CleanupFailure" {
						return status.Error(codes.Unavailable, "Cleanup failed")
					}
					return nil
				}
			}
			err := program.RunLocal(ctx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					<-ctx.Done()
					close(dependencyStopped)
					return nil
				})
				builder.LaunchWorkerThread(siblingsGroup, client, "test", cleanup)
				return nil
			})
			if mode == "CleanupFailure" {
				require.Error(test, err)
			} else {
				require.NoError(test, err)
			}
			<-executionFinished
			if cleanup != nil {
				<-cleanupFinished
			}
			<-dependencyStopped
		})
	}
}
