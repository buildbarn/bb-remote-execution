package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCostComputingBuildExecutorSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)

	startTime := &timestamppb.Timestamp{Nanos: 0}
	endTime := &timestamppb.Timestamp{Seconds: 1, Nanos: 500000000}

	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				WorkerStartTimestamp:     startTime,
				WorkerCompletedTimestamp: endTime,
				AuxiliaryMetadata:        []*anypb.Any{},
			},
		},
	})
	monetaryResourceUsage, err := anypb.New(&resourceusage.MonetaryResourceUsage{
		Expenses: map[string]*resourceusage.MonetaryResourceUsage_Expense{
			"EC2":         {Currency: "USD", Cost: 0.1851},
			"S3":          {Currency: "BTC", Cost: 0.885},
			"Electricity": {Currency: "EUR", Cost: 1.845},
			"Maintenance": {Currency: "JPY", Cost: 0.18},
		},
	})
	require.NoError(t, err)

	costComputingBuildExecutor := builder.NewCostComputingBuildExecutor(baseBuildExecutor, map[string]*resourceusage.MonetaryResourceUsage_Expense{
		"EC2":         {Currency: "USD", Cost: 0.1234},
		"S3":          {Currency: "BTC", Cost: 0.59},
		"Electricity": {Currency: "EUR", Cost: 1.23},
		"Maintenance": {Currency: "JPY", Cost: 0.12},
	})

	executeResponse := costComputingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				WorkerStartTimestamp:     startTime,
				WorkerCompletedTimestamp: endTime,
				AuxiliaryMetadata: []*anypb.Any{
					monetaryResourceUsage,
				},
			},
		},
	}, executeResponse)
}
