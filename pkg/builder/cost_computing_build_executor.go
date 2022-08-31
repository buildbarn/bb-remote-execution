package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/anypb"
)

type costComputingBuildExecutor struct {
	BuildExecutor
	pricingPerSecond map[string]*resourceusage.MonetaryResourceUsage_Expense
}

// NewCostComputingBuildExecutor wraps an existing BuildExecutor, adding the computed
// cost of the action to the prepopulated AuxiliaryMetadata field of the ActionResult.
// The provided expenses are represented on a per-second basis and are then multiplied
// by the amount of seconds that it took for a worker to complete the Action.
func NewCostComputingBuildExecutor(base BuildExecutor, expensesPerSecond map[string]*resourceusage.MonetaryResourceUsage_Expense) BuildExecutor {
	return &costComputingBuildExecutor{
		BuildExecutor:    base,
		pricingPerSecond: expensesPerSecond,
	}
}

func (be *costComputingBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.BuildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates)

	totalTime := response.Result.ExecutionMetadata.WorkerCompletedTimestamp.AsTime().Sub(response.Result.ExecutionMetadata.WorkerStartTimestamp.AsTime()).Seconds()
	costsPerSecond := resourceusage.MonetaryResourceUsage{
		Expenses: map[string]*resourceusage.MonetaryResourceUsage_Expense{},
	}
	for costType, exp := range be.pricingPerSecond {
		costsPerSecond.Expenses[costType] = &resourceusage.MonetaryResourceUsage_Expense{
			Currency: exp.Currency,
			Cost:     totalTime * exp.Cost,
		}
	}

	if monetaryResourceUsage, err := anypb.New(&costsPerSecond); err == nil {
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, monetaryResourceUsage)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to marshal monetary resource usage"))
	}
	return response
}
