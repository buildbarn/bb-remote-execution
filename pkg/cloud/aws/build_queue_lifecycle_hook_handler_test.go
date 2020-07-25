package aws_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildQueueLifecycleHookHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	ec2Service := mock.NewMockEC2(ctrl)
	buildQueue := mock.NewMockBuildQueueStateProvider(ctrl)
	lhh := re_aws.NewBuildQueueLifecycleHookHandler(ec2Service, buildQueue, "hostname")

	t.Run("RecentlyTerminatedInstance", func(t *testing.T) {
		// For instances that were already terminated recently,
		// DescribeInstances() will return an empty reservations
		// list. There is nothing we can do anymore.
		ec2Service.EXPECT().DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String("i-48591284758294845")},
		}).Return(&ec2.DescribeInstancesOutput{}, nil)

		require.NoError(t, lhh.HandleEC2InstanceTerminating("i-48591284758294845"))
	})

	t.Run("HistoricallyTerminatedInstance", func(t *testing.T) {
		// For instances that were terminated a long time ago,
		// DescribeInstances() returns an explicit error. This
		// error should not get propagated.
		ec2Service.EXPECT().DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String("i-34984598234581744")},
		}).Return(nil, awserr.New("InvalidInstanceID.NotFound", "The instance ID 'i-34984598234581744' does not exist", nil))

		require.NoError(t, lhh.HandleEC2InstanceTerminating("i-34984598234581744"))
	})

	t.Run("Failure", func(t *testing.T) {
		// Hard failures of the EC2 API should cause the handler
		// to fail, so that it may be retried later.
		ec2Service.EXPECT().DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String("i-59438147357578398")},
		}).Return(nil, awserr.New("ServiceUnavailable", "Received a HTTP 503", nil))

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to describe EC2 instance \"i-59438147357578398\": ServiceUnavailable: Received a HTTP 503"),
			lhh.HandleEC2InstanceTerminating("i-59438147357578398"))
	})

	t.Run("Success", func(t *testing.T) {
		// Successful resolution of the private DNS name of the
		// EC2 instance allows us to mark the workers as
		// terminating and wait for build actions to complete.
		ec2Service.EXPECT().DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String("i-59438147357578398")},
		}).Return(&ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				{
					Instances: []*ec2.Instance{
						{
							InstanceId:     aws.String("i-59438147357578398"),
							PrivateDnsName: aws.String("ip-10-72-82-5.eu-west-1.compute.internal"),
						},
					},
				},
			},
		}, nil)
		buildQueue.EXPECT().MarkTerminatingAndWait(map[string]string{
			"hostname": "ip-10-72-82-5.eu-west-1.compute.internal",
		})

		require.NoError(t, lhh.HandleEC2InstanceTerminating("i-59438147357578398"))
	})
}
