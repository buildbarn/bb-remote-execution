package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTestInfrastructureFailureDetectingBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	buildExecutor := builder.NewTestInfrastructureFailureDetectingBuildExecutor(
		baseBuildExecutor,
		builder.NewTestInfrastructureFailureShutdownState(),
		/* maximumConsecutiveFailures = */ 5)

	// Common values used by the tests below.
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_MD5)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "c7af09d7f0c45d36b46e21616398a1eb",
			SizeBytes: 100,
		},
		Action: &remoteexecution.Action{},
	}
	failedResponse := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			OutputFiles: []*remoteexecution.OutputFile{
				{Path: "bazel-out/linux_x86_64/testlogs/my/test/test.infrastructure_failure"},
			},
		},
	}
	successfulResponse := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			OutputFiles: []*remoteexecution.OutputFile{
				{Path: "bazel-out/linux_x86_64/testlogs/my/test/test.outputs/outputs.zip"},
			},
		},
	}

	// By default, calls to CheckReadiness should just be forwarded
	// to the underlying BuildExecutor.
	baseBuildExecutor.EXPECT().CheckReadiness(gomock.Any()).
		Return(status.Error(codes.Internal, "Runner unavailable"))
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Runner unavailable"), buildExecutor.CheckReadiness(ctx))

	baseBuildExecutor.EXPECT().CheckReadiness(gomock.Any())
	require.NoError(t, buildExecutor.CheckReadiness(ctx))

	// Execute a couple of tests that trigger infrastructure
	// failures. As this is still right below the configured limit,
	// this shouldn't mark the worker in an unhealthy state.
	for i := 0; i < 4; i++ {
		baseBuildExecutor.EXPECT().Execute(gomock.Any(), filePool, monitor, digestFunction, request, metadata).Return(failedResponse)
		testutil.RequireEqualProto(t, failedResponse, buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata))
	}

	baseBuildExecutor.EXPECT().CheckReadiness(gomock.Any())
	require.NoError(t, buildExecutor.CheckReadiness(ctx))

	// Now reset the counter of consecutive infrastructure failures by
	// executing a successful test.
	baseBuildExecutor.EXPECT().Execute(gomock.Any(), filePool, monitor, digestFunction, request, metadata).Return(successfulResponse)
	testutil.RequireEqualProto(t, successfulResponse, buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata))

	baseBuildExecutor.EXPECT().CheckReadiness(gomock.Any())
	require.NoError(t, buildExecutor.CheckReadiness(ctx))

	// We may once again trigger a number of tests without marking
	// the worker unhealthy.
	for i := 0; i < 4; i++ {
		baseBuildExecutor.EXPECT().Execute(gomock.Any(), filePool, monitor, digestFunction, request, metadata).Return(failedResponse)
		testutil.RequireEqualProto(t, failedResponse, buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata))
	}

	baseBuildExecutor.EXPECT().CheckReadiness(gomock.Any())
	require.NoError(t, buildExecutor.CheckReadiness(ctx))

	// Running a fifth failing test should cause the worker to be
	// marked unhealthy.
	baseBuildExecutor.EXPECT().Execute(gomock.Any(), filePool, monitor, digestFunction, request, metadata).Return(failedResponse)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
			OutputFiles: []*remoteexecution.OutputFile{
				{Path: "bazel-out/linux_x86_64/testlogs/my/test/test.infrastructure_failure"},
			},
		},
		Status: status.New(codes.Unavailable, "Worker has shut down, as too many consecutive tests reported an infrastructure failure").Proto(),
	}, buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata))

	// Future readiness checks and execution requests should fail.
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Worker has shut down, as too many consecutive tests reported an infrastructure failure"), buildExecutor.CheckReadiness(ctx))
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Unavailable, "Worker has shut down, as too many consecutive tests reported an infrastructure failure").Proto(),
	}, buildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata))
}
