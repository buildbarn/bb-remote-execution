package aws_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/smithy-go"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLifecycleHookSQSMessageHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	autoScalingClient := mock.NewMockAutoScalingClient(ctrl)
	handler := mock.NewMockLifecycleHookHandler(ctrl)
	smh := re_aws.NewLifecycleHookSQSMessageHandler(autoScalingClient, handler)

	t.Run("BadBody", func(t *testing.T) {
		// Received a non-JSON message. Discard it to prevent us
		// from retrying it indefinitely.
		require.NoError(t, smh.HandleMessage("This is not JSON"))
	})

	t.Run("TestMessage", func(t *testing.T) {
		// Lifecycle hooks created through CloudFormation may
		// cause test notifications like the following to be
		// sent. These should be ignored.
		require.NoError(
			t,
			smh.HandleMessage(`{
				"AccountId": "349895348295",
				"AutoScalingGroupARN": "MyClusterASGARN",
				"AutoScalingGroupName": "MyClusterASGName",
				"Event": "autoscaling:TEST_NOTIFICATION",
				"RequestId": "5c605812-5778-4fc3-a5b0-68beabf3d05f",
				"Service": "AWS Auto Scaling",
				"Time": "2020-01-01T01:01:00.000Z"
			}`))
	})

	t.Run("EC2InstanceTerminatingSuccess", func(t *testing.T) {
		// Full flow of a EC2_INSTANCE_TERMINATING lifecycle
		// event that gets processed and acknowledged.
		handler.EXPECT().HandleEC2InstanceTerminating("i-59042803948349524")
		autoScalingClient.EXPECT().CompleteLifecycleAction(gomock.Any(), &autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String("MyClusterASGName"),
			LifecycleActionResult: aws.String("CONTINUE"),
			LifecycleActionToken:  aws.String("2c0d4442-c2bf-4ee5-9685-d8c11f840723"),
			LifecycleHookName:     aws.String("MyClusterTerminationHook"),
		}).Return(&autoscaling.CompleteLifecycleActionOutput{}, nil)

		require.NoError(
			t,
			smh.HandleMessage(`{
				"AccountId": "059843095823",
				"AutoScalingGroupName": "MyClusterASGName",
				"EC2InstanceId": "i-59042803948349524",
				"LifecycleActionToken": "2c0d4442-c2bf-4ee5-9685-d8c11f840723",
				"LifecycleHookName": "MyClusterTerminationHook",
				"LifecycleTransition": "autoscaling:EC2_INSTANCE_TERMINATING",
				"RequestId": "0ce01006-a243-406a-8e5a-5b8bd6cfe587",
				"Service": "AWS Auto Scaling",
				"Time": "2020-01-01T01:01:00.000Z"
			}`))
	})

	t.Run("EC2InstanceTerminatingHandlerFailure", func(t *testing.T) {
		// The handler for EC2 instance termination events had a
		// failure. We should not call out to complete the
		// lifecycle action.
		handler.EXPECT().HandleEC2InstanceTerminating("i-59042803948349524").
			Return(status.Error(codes.Internal, "Failed to release EC2 instance"))

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to release EC2 instance"),
			smh.HandleMessage(`{
				"AccountId": "059843095823",
				"AutoScalingGroupName": "MyClusterASGName",
				"EC2InstanceId": "i-59042803948349524",
				"LifecycleActionToken": "2c0d4442-c2bf-4ee5-9685-d8c11f840723",
				"LifecycleHookName": "MyClusterTerminationHook",
				"LifecycleTransition": "autoscaling:EC2_INSTANCE_TERMINATING",
				"RequestId": "0ce01006-a243-406a-8e5a-5b8bd6cfe587",
				"Service": "AWS Auto Scaling",
				"Time": "2020-01-01T01:01:00.000Z"
			}`))
	})

	t.Run("EC2InstanceTerminatingCompletionStale", func(t *testing.T) {
		// An EC2_INSTANCE_TERMINATING lifecycle event that
		// already spent so much time in the queue, that it can
		// no longer be completed. These errors should be
		// discarded to prevent us from retrying them
		// indefinitely.
		handler.EXPECT().HandleEC2InstanceTerminating("i-59042803948349524")
		validationError := mock.NewMockAPIError(ctrl)
		autoScalingClient.EXPECT().CompleteLifecycleAction(gomock.Any(), &autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String("MyClusterASGName"),
			LifecycleActionResult: aws.String("CONTINUE"),
			LifecycleActionToken:  aws.String("2c0d4442-c2bf-4ee5-9685-d8c11f840723"),
			LifecycleHookName:     aws.String("MyClusterTerminationHook"),
		}).Return(nil, validationError)
		validationError.EXPECT().ErrorCode().Return("ValidationError").AnyTimes()

		require.NoError(
			t,
			smh.HandleMessage(`{
				"AccountId": "059843095823",
				"AutoScalingGroupName": "MyClusterASGName",
				"EC2InstanceId": "i-59042803948349524",
				"LifecycleActionToken": "2c0d4442-c2bf-4ee5-9685-d8c11f840723",
				"LifecycleHookName": "MyClusterTerminationHook",
				"LifecycleTransition": "autoscaling:EC2_INSTANCE_TERMINATING",
				"RequestId": "0ce01006-a243-406a-8e5a-5b8bd6cfe587",
				"Service": "AWS Auto Scaling",
				"Time": "2020-01-01T01:01:00.000Z"
			}`))
	})

	t.Run("EC2InstanceTerminatingCompletionFailure", func(t *testing.T) {
		// We successfully ran the the handler, but failed to
		// complete the lifehook action. This error should be
		// propagated, so that termination is retried.
		handler.EXPECT().HandleEC2InstanceTerminating("i-59042803948349524")
		autoScalingClient.EXPECT().CompleteLifecycleAction(gomock.Any(), &autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String("MyClusterASGName"),
			LifecycleActionResult: aws.String("CONTINUE"),
			LifecycleActionToken:  aws.String("2c0d4442-c2bf-4ee5-9685-d8c11f840723"),
			LifecycleHookName:     aws.String("MyClusterTerminationHook"),
		}).Return(nil, &smithy.OperationError{
			ServiceID:     "autoscaling",
			OperationName: "CompleteLifecycleAction",
			Err:           errors.New("received a HTTP 503"),
		})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to complete lifecycle action: operation error autoscaling: CompleteLifecycleAction, received a HTTP 503"),
			smh.HandleMessage(`{
				"AccountId": "059843095823",
				"AutoScalingGroupName": "MyClusterASGName",
				"EC2InstanceId": "i-59042803948349524",
				"LifecycleActionToken": "2c0d4442-c2bf-4ee5-9685-d8c11f840723",
				"LifecycleHookName": "MyClusterTerminationHook",
				"LifecycleTransition": "autoscaling:EC2_INSTANCE_TERMINATING",
				"RequestId": "0ce01006-a243-406a-8e5a-5b8bd6cfe587",
				"Service": "AWS Auto Scaling",
				"Time": "2020-01-01T01:01:00.000Z"
			}`))
	})
}
