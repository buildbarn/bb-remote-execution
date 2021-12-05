package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	mock "github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActionLoggingBuildExecutor(t *testing.T) {
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
	instanceName := digest.MustNewInstanceName("freebsd12")
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)

	executeResponse := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				WorkerStartTimestamp:     &timestamppb.Timestamp{Nanos: 0},
				WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 500000000},
				AuxiliaryMetadata:        []*anypb.Any{},
			},
		},
	}

	baseBuildExecutor.EXPECT().Execute(ctx, filePool, instanceName, request, metadata).Return(executeResponse)

	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	lq := mock.NewMockCompletedActionLogger(ctrl)
	completedActionLoggingBuildExecutor := builder.NewCompletedActionLoggingBuildExecutor(
		baseBuildExecutor,
		uuidGenerator.Call,
		lq,
		digest.NewInstanceNamePatcher(digest.EmptyInstanceName, digest.MustNewInstanceName("prefix")))

	uuidGenerator.EXPECT().Call().Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))
	lq.EXPECT().LogCompletedAction(&cal_proto.CompletedAction{
		HistoricalExecuteResponse: &cas_proto.HistoricalExecuteResponse{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			},
			ExecuteResponse: executeResponse,
		},
		Uuid:         "36ebab65-3c4f-4faf-818b-2eabb4cd1b02",
		InstanceName: "prefix/freebsd12",
	})
	resp := completedActionLoggingBuildExecutor.Execute(ctx, filePool, instanceName, request, metadata)

	testutil.RequireEqualProto(t, resp, executeResponse)
}
