package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type tracingBuildExecutor struct {
	BuildExecutor
	tracer trace.Tracer
}

// NewTracingBuildExecutor is a decorator for BuildExecutor that creates
// an OpenTelemetry trace span for every action that is executed. At the
// start of every execution state, an event is added to the span that
// indicates which state is entered.
func NewTracingBuildExecutor(buildExecutor BuildExecutor, tracerProvider trace.TracerProvider) BuildExecutor {
	return &tracingBuildExecutor{
		BuildExecutor: buildExecutor,
		tracer:        tracerProvider.Tracer("github.com/buildbarn/bb-remote-execution/pkg/builder"),
	}
}

func (be *tracingBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	actionDigest := request.ActionDigest
	action := request.Action
	ctxWithTracing, span := be.tracer.Start(ctx, "BuildExecutor.Execute", trace.WithAttributes(
		attribute.String("action_digest.hash", actionDigest.GetHash()),
		attribute.Int64("action_digest.size_bytes", actionDigest.GetSizeBytes()),
		attribute.Bool("do_not_cache", action.GetDoNotCache()),
		attribute.String("instance_name", instanceName.String()),
		attribute.Float64("timeout", action.GetTimeout().AsDuration().Seconds()),
	))
	defer span.End()

	baseUpdates := make(chan *remoteworker.CurrentState_Executing)
	baseCompletion := make(chan *remoteexecution.ExecuteResponse)
	go func() {
		baseCompletion <- be.BuildExecutor.Execute(ctxWithTracing, filePool, instanceName, request, baseUpdates)
	}()

	for {
		select {
		case update := <-baseUpdates:
			switch update.ExecutionState.(type) {
			case *remoteworker.CurrentState_Executing_FetchingInputs:
				span.AddEvent("FetchingInputs")
			case *remoteworker.CurrentState_Executing_Running:
				span.AddEvent("Running")
			case *remoteworker.CurrentState_Executing_UploadingOutputs:
				span.AddEvent("UploadingOutputs")
			}

			executionStateUpdates <- update
		case response := <-baseCompletion:
			return response
		}
	}
}
