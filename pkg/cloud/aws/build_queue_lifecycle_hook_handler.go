package aws

import (
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
)

type buildQueueLifecycleHookHandler struct {
	buildQueue      builder.BuildQueueStateProvider
	instanceIDLabel string
}

// NewBuildQueueLifecycleHookHandler creates a new lifecycle hook
// handler that forwards EC2 instance termination events to a build
// queue. This causes the build queue to drain any workers running on
// this EC2 instance, while ensuring that the EC2 instance is not
// terminated before existing operations finish.
func NewBuildQueueLifecycleHookHandler(buildQueue builder.BuildQueueStateProvider, instanceIDLabel string) LifecycleHookHandler {
	return &buildQueueLifecycleHookHandler{
		buildQueue:      buildQueue,
		instanceIDLabel: instanceIDLabel,
	}
}

func (lhh *buildQueueLifecycleHookHandler) HandleEC2InstanceTerminating(instanceID string) {
	lhh.buildQueue.MarkTerminatingAndWait(map[string]string{
		lhh.instanceIDLabel: instanceID,
	})
}
