package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// EC2 is an interface around the AWS SDK EC2 client. It contains the
// operations that are used by BuildQueueLifeCycleHookHandler. It is
// present to aid unit testing.
type EC2 interface {
	DescribeInstances(input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error)
}

var _ EC2 = (*ec2.EC2)(nil)

type buildQueueLifecycleHookHandler struct {
	ec2                 EC2
	buildQueue          builder.BuildQueueStateProvider
	privateDNSNameLabel string
}

// NewBuildQueueLifecycleHookHandler creates a new lifecycle hook
// handler that forwards EC2 instance termination events to a build
// queue. This causes the build queue to drain any workers running on
// this EC2 instance, while ensuring that the EC2 instance is not
// terminated before existing operations finish.
func NewBuildQueueLifecycleHookHandler(ec2 EC2, buildQueue builder.BuildQueueStateProvider, privateDNSNameLabel string) LifecycleHookHandler {
	return &buildQueueLifecycleHookHandler{
		ec2:                 ec2,
		buildQueue:          buildQueue,
		privateDNSNameLabel: privateDNSNameLabel,
	}
}

func (lhh *buildQueueLifecycleHookHandler) HandleEC2InstanceTerminating(instanceID string) error {
	// People tend to use EC2 instance hostnames as part of their
	// worker IDs; not the EC2 instance IDs themselves. Use the EC2
	// API to resolve the instance ID to its hostname.
	describeInstancesOutput, err := lhh.ec2.DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(instanceID)},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "InvalidInstanceID.NotFound" {
			// The instance has already been terminated.
			return nil
		}
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to describe EC2 instance %#v", instanceID)
	}
	for _, reservation := range describeInstancesOutput.Reservations {
		for _, instance := range reservation.Instances {
			lhh.buildQueue.MarkTerminatingAndWait(map[string]string{
				lhh.privateDNSNameLabel: *instance.PrivateDnsName,
			})
		}
	}
	return nil
}
