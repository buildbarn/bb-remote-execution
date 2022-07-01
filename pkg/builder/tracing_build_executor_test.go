package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"

	"google.golang.org/protobuf/types/known/durationpb"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestTracingBuildExecutor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Creating an instance of TracingBuildExecutor should cause it
	// to create a new Tracer object.
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	tracerProvider := mock.NewMockTracerProvider(ctrl)
	tracer := mock.NewMockTracer(ctrl)
	tracerProvider.EXPECT().Tracer("github.com/buildbarn/bb-remote-execution/pkg/builder").Return(tracer)

	buildExecutor := builder.NewTracingBuildExecutor(baseBuildExecutor, tracerProvider)

	// Example execution request, response and execution state updates.
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "caa9adf60f3b5fd05d7cb6f17bac9201ad9d444d01e7b6964901055e6d6a5c4b",
			SizeBytes: 142,
		},
		Action: &remoteexecution.Action{
			DoNotCache: true,
			Timeout:    &durationpb.Duration{Seconds: 5},
		},
	}
	response := &remoteexecution.ExecuteResponse{}
	fetchingInputs := &remoteworker.CurrentState_Executing{
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{},
	}
	running := &remoteworker.CurrentState_Executing{
		ExecutionState: &remoteworker.CurrentState_Executing_Running{},
	}
	uploadingOutputs := &remoteworker.CurrentState_Executing{
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{},
	}

	// Call Execute() against the TracingBuildExecutor. The call
	// should be forwarded to the underlying BuildExecutor in
	// literal form, and execution state updates should also be
	// forwarded back to the caller. A trace span should be created
	// that contains events for each of the execution state updates.
	ctxWithTracing := mock.NewMockContext(ctrl)
	filePool := mock.NewMockFilePool(ctrl)
	instanceName := digest.MustNewInstanceName("hello")
	baseBuildExecutor.EXPECT().Execute(ctxWithTracing, filePool, instanceName, testutil.EqProto(t, request), gomock.Any()).DoAndReturn(
		func(ctx context.Context, filePool re_filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
			executionStateUpdates <- fetchingInputs
			executionStateUpdates <- running
			executionStateUpdates <- uploadingOutputs
			return response
		})

	span := mock.NewMockSpan(ctrl)
	tracer.EXPECT().Start(ctx, "BuildExecutor.Execute", trace.WithAttributes(
		attribute.String("action_digest.hash", "caa9adf60f3b5fd05d7cb6f17bac9201ad9d444d01e7b6964901055e6d6a5c4b"),
		attribute.Int64("action_digest.size_bytes", 142),
		attribute.Bool("do_not_cache", true),
		attribute.String("instance_name", "hello"),
		attribute.Float64("timeout", 5),
	)).Return(ctxWithTracing, span)
	span.EXPECT().AddEvent("FetchingInputs")
	span.EXPECT().AddEvent("Running")
	span.EXPECT().AddEvent("UploadingOutputs")
	span.EXPECT().End()

	executionStateUpdates := make(chan *remoteworker.CurrentState_Executing, 3)
	testutil.RequireEqualProto(t, response, buildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates))
	testutil.RequireEqualProto(t, fetchingInputs, <-executionStateUpdates)
	testutil.RequireEqualProto(t, running, <-executionStateUpdates)
	testutil.RequireEqualProto(t, uploadingOutputs, <-executionStateUpdates)
}
