package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/cas"
	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
)

type completedActionLoggingBuildExecutor struct {
	BuildExecutor
	uuidGenerator       util.UUIDGenerator
	logger              CompletedActionLogger
	instanceNamePatcher digest.InstanceNamePatcher
}

// NewCompletedActionLoggingBuildExecutor returns a new
// completedActionLoggingBuildExecutor that will transmit CompletedActions
// to an external server for real-time analysis of REv2 Action metadata
// using a CompletedActionLogger.
func NewCompletedActionLoggingBuildExecutor(base BuildExecutor, uuidGenerator util.UUIDGenerator, logger CompletedActionLogger, instanceNamePatcher digest.InstanceNamePatcher) BuildExecutor {
	return &completedActionLoggingBuildExecutor{
		BuildExecutor:       base,
		uuidGenerator:       uuidGenerator,
		logger:              logger,
		instanceNamePatcher: instanceNamePatcher,
	}
}

func (be *completedActionLoggingBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.BuildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates)

	completedAction := &cal_proto.CompletedAction{
		HistoricalExecuteResponse: &cas_proto.HistoricalExecuteResponse{
			ActionDigest:    request.ActionDigest,
			ExecuteResponse: response,
		},
		Uuid:         uuid.Must(be.uuidGenerator()).String(),
		InstanceName: be.instanceNamePatcher.PatchInstanceName(instanceName).String(),
	}

	be.logger.LogCompletedAction(completedAction)
	return response
}
