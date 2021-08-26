package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
)

type completedActionLoggingBuildExecutor struct {
	base          BuildExecutor
	logger        CompletedActionLogger
	uuidGenerator util.UUIDGenerator
}

// NewCompletedActionLoggingBuildExecutor returns a new
// completedActionLoggingBuildExecutor that will transmit CompletedActions
// to an external server for real-time analysis of REv2 Action metadata
// using a CompletedActionLogger.
func NewCompletedActionLoggingBuildExecutor(base BuildExecutor, uuidGenerator util.UUIDGenerator, logger CompletedActionLogger) BuildExecutor {
	return &completedActionLoggingBuildExecutor{
		base:          base,
		logger:        logger,
		uuidGenerator: uuidGenerator,
	}
}

func (be *completedActionLoggingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.base.Execute(ctx, filePool, instanceName, request, executionStateUpdates)

	completedAction := &cal_proto.CompletedAction{
		HistoricalExecuteResponse: &cas_proto.HistoricalExecuteResponse{
			ActionDigest:    request.ActionDigest,
			ExecuteResponse: response,
		},
		Uuid:         uuid.Must(be.uuidGenerator()).String(),
		InstanceName: instanceName.String(),
	}

	be.logger.LogCompletedAction(completedAction)
	return response
}
