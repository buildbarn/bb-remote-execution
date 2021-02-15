package aws

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// AutoScaling is an interface around the AWS SDK Auto Scaling client.
// It contains the operations that are used by
// LifecycleHookSQSMessageHandler. It is present to aid unit testing.
type AutoScaling interface {
	CompleteLifecycleAction(input *autoscaling.CompleteLifecycleActionInput) (*autoscaling.CompleteLifecycleActionOutput, error)
}

var _ AutoScaling = (*autoscaling.AutoScaling)(nil)

// LifecycleHookHandler is called into by LifecycleHookSQSMessageHandler
// for every valid lifecycle event message received through SQS. Right
// now only termination events are forwarded.
type LifecycleHookHandler interface {
	HandleEC2InstanceTerminating(instanceID string) error
}

// lifecycleHookMessage is a message that Amazon Auto Scaling Groups
// (ASGs) sends over a lifecycle hook when terminating an instance.
type lifecycleHookMessage struct {
	LifecycleTransition string `json:"LifecycleTransition"`

	// The ID of the EC2 instance that is being terminated. This is
	// passed on to the LifecycleHookHandler.
	EC2InstanceID string `json:"EC2InstanceId"`

	// Fields that we need to provide to CompleteLifecycleAction()
	// after successfully draining the EC2 instance.
	AutoScalingGroupName string `json:"AutoScalingGroupName"`
	LifecycleActionToken string `json:"LifecycleActionToken"`
	LifecycleHookName    string `json:"LifecycleHookName"`
}

type lifecycleHookSQSMessageHandler struct {
	autoScaling AutoScaling
	handler     LifecycleHookHandler
}

// NewLifecycleHookSQSMessageHandler creates an SQSMessageHandler that
// assumes that messages received through SQS contain events generated
// by an ASG lifecycle hook. These events may be generated when EC2
// instances are added and removed from Auto Scaling Groups (ASGs).
func NewLifecycleHookSQSMessageHandler(autoScaling AutoScaling, handler LifecycleHookHandler) SQSMessageHandler {
	return &lifecycleHookSQSMessageHandler{
		autoScaling: autoScaling,
		handler:     handler,
	}
}

func (smh *lifecycleHookSQSMessageHandler) HandleMessage(body string) error {
	// Discard any messages that are not for EC2 instance
	// termination.
	var message lifecycleHookMessage
	if err := json.Unmarshal([]byte(body), &message); err != nil {
		return nil
	}
	if message.LifecycleTransition != "autoscaling:EC2_INSTANCE_TERMINATING" {
		return nil
	}

	// Invoke the handler.
	if err := smh.handler.HandleEC2InstanceTerminating(message.EC2InstanceID); err != nil {
		return err
	}

	// Allow AWS to go ahead with termination of the EC2 instance.
	if _, err := smh.autoScaling.CompleteLifecycleAction(&autoscaling.CompleteLifecycleActionInput{
		AutoScalingGroupName:  aws.String(message.AutoScalingGroupName),
		LifecycleActionResult: aws.String("CONTINUE"),
		LifecycleActionToken:  aws.String(message.LifecycleActionToken),
		LifecycleHookName:     aws.String(message.LifecycleHookName),
	}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() != "ValidationError" {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to complete lifecycle action")
		}
	}
	return nil
}
