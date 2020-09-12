package aws_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/golang/mock/gomock"
)

func TestBuildQueueLifecycleHookHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildQueue := mock.NewMockBuildQueueStateProvider(ctrl)
	lhh := re_aws.NewBuildQueueLifecycleHookHandler(buildQueue, "instance")

	buildQueue.EXPECT().MarkTerminatingAndWait(map[string]string{
		"instance": "i-59438147357578398",
	})
	lhh.HandleEC2InstanceTerminating("i-59438147357578398")
}
