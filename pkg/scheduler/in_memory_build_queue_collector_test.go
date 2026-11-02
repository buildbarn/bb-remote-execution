package scheduler_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

// expectedSizeClassQueueGauges returns the gauges that InMemoryBuildQueue
// should report for the size class queue of platformForTesting, in the
// text exposition format.
func expectedSizeClassQueueGauges(queued, executing, workers int) string {
	labels := `{instance_name_prefix="main",platform=` +
		strconv.Quote(platform.MustNewKey("main", platformForTesting).GetPlatformString()) +
		`,size_class="0"} `
	return `
# HELP buildbarn_builder_in_memory_build_queue_tasks_executing Number of tasks that are currently executing on a worker.
# TYPE buildbarn_builder_in_memory_build_queue_tasks_executing gauge
buildbarn_builder_in_memory_build_queue_tasks_executing` + labels + strconv.Itoa(executing) + `
# HELP buildbarn_builder_in_memory_build_queue_tasks_queued Number of tasks that are currently queued, waiting for a worker.
# TYPE buildbarn_builder_in_memory_build_queue_tasks_queued gauge
buildbarn_builder_in_memory_build_queue_tasks_queued` + labels + strconv.Itoa(queued) + `
# HELP buildbarn_builder_in_memory_build_queue_workers Number of workers that are currently registered.
# TYPE buildbarn_builder_in_memory_build_queue_workers gauge
buildbarn_builder_in_memory_build_queue_workers` + labels + strconv.Itoa(workers) + `
`
}

func TestInMemoryBuildQueueCollect(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionReader := mock.NewMockMessageReader[*remoteexecution.Action](ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().
		Now().
		Return(time.Unix(0, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	actionRouter := mock.NewMockActionRouter(ctrl)
	buildQueue := scheduler.NewInMemoryBuildQueue(actionReader, clock, uuidGenerator.Call, &buildQueueConfigurationForTesting, actionRouter, allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer)
	executionClient := getExecutionClient(t, buildQueue)

	// Without any size class queue, nothing is reported.
	clock.EXPECT().
		Now().
		Return(time.Unix(1000, 0))
	require.Equal(t, 0, promtestutil.CollectAndCount(buildQueue))

	// A predeclared platform queue has a size class queue, even
	// without any workers.
	clock.EXPECT().
		Now().
		Return(time.Unix(1001, 0))
	require.NoError(t, buildQueue.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		/* workerInvocationStickinessLimits = */ nil,
		/* maximumQueuedBackgroundLearningOperations = */ 0,
		/* backgroundLearningOperationPriority = */ 0,
		/* sizeClasses = */ []uint32{0},
	))
	clock.EXPECT().
		Now().
		Return(time.Unix(1002, 0))
	require.NoError(t, promtestutil.CollectAndCompare(buildQueue, strings.NewReader(expectedSizeClassQueueGauges(0, 0, 0))))

	// Let a client enqueue an operation, which remains queued as
	// no worker is available.
	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
			SizeBytes: 456,
		},
	}
	actionReader.EXPECT().
		ReadMessage(
			gomock.Any(),
			digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, "da39a3ee5e6b4b0d3255bfef95601890afd80709", 123),
		).
		Return(action, nil)
	initialSizeClassSelector := mock.NewMockSelector(ctrl)
	actionRouter.EXPECT().
		RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action), nil).
		Return(action, platform.MustNewKey("main", platformForTesting), nil, initialSizeClassSelector, nil)
	initialSizeClassLearner := mock.NewMockLearner(ctrl)
	initialSizeClassSelector.EXPECT().
		Select([]uint32{0}).
		Return(0, 3*time.Minute, 7*time.Minute, initialSizeClassLearner)
	clock.EXPECT().
		Now().
		Return(time.Unix(1003, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clock.EXPECT().
		NewTimer(time.Minute).
		Return(timer1, nil)
	uuidGenerator.EXPECT().
		Call().
		Return(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

	stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.NoError(t, err)

	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	require.NoError(t, promtestutil.CollectAndCompare(buildQueue, strings.NewReader(expectedSizeClassQueueGauges(1, 0, 0))))

	// Let a worker pick up the operation, which then executes.
	timer1.EXPECT().
		Stop().
		Return(true)
	clock.EXPECT().
		Now().
		Return(time.Unix(1005, 0)).
		Times(2)

	timer2 := mock.NewMockTimer(ctrl)
	clock.EXPECT().
		NewTimer(time.Minute).
		Return(timer2, nil)

	response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId: map[string]string{
			"hostname": "worker123",
			"thread":   "42",
		},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, response.DesiredState.GetExecuting())

	clock.EXPECT().
		Now().
		Return(time.Unix(1006, 0))
	require.NoError(t, promtestutil.CollectAndCompare(buildQueue, strings.NewReader(expectedSizeClassQueueGauges(0, 1, 1))))
}
