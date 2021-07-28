package aws_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_aws "github.com/buildbarn/bb-remote-execution/pkg/cloud/aws"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSQSReceiver(t *testing.T) {
	ctrl := gomock.NewController(t)

	sqsService := mock.NewMockSQS(ctrl)
	messageHandler := mock.NewMockSQSMessageHandler(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	sr := re_aws.NewSQSReceiver(
		sqsService,
		"https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue",
		10*time.Minute,
		messageHandler,
		errorLogger)

	t.Run("ReceiveMessageFailure", func(t *testing.T) {
		// Failures to read from SQS can be returned
		// immediately, as this happens in the foreground.
		sqsService.EXPECT().ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(600),
			WaitTimeSeconds:     aws.Int64(20),
		}).Return(nil, awserr.New("ServiceUnavailable", "Received a HTTP 503", nil))

		require.Equal(
			t,
			awserr.New("ServiceUnavailable", "Received a HTTP 503", nil),
			sr.PerformSingleRequest())
	})

	t.Run("HandlerFailure", func(t *testing.T) {
		// Because handlers are executed asynchronously, any
		// errors should be passed to the provided ErrorLogger.
		sqsService.EXPECT().ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(600),
			WaitTimeSeconds:     aws.Int64(20),
		}).Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{
				{
					Body:          aws.String("This is a message body"),
					MessageId:     aws.String("8dcc80c7-83ed-4d3c-aa38-59342fd192f8"),
					ReceiptHandle: aws.String("15066bf8-c753-4788-813a-18e607d209f9"),
				},
			},
		}, nil)
		messageHandler.EXPECT().HandleMessage("This is a message body").
			Return(status.Error(codes.Internal, "Cannot contact backend service"))
		testCompleted := make(chan struct{})
		errorLogger.EXPECT().
			Log(status.Error(codes.Internal, "Failed to process message \"8dcc80c7-83ed-4d3c-aa38-59342fd192f8\": Cannot contact backend service")).
			Do(func(err error) { close(testCompleted) })

		require.NoError(t, sr.PerformSingleRequest())
		<-testCompleted
	})

	t.Run("DeleteMessageFailure", func(t *testing.T) {
		// After processing the message, it should be deleted
		// from the queue. Deletion errors should also be passed
		// to the ErrorLogger.
		sqsService.EXPECT().ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(600),
			WaitTimeSeconds:     aws.Int64(20),
		}).Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{
				{
					Body:          aws.String("This is a message body"),
					MessageId:     aws.String("8dcc80c7-83ed-4d3c-aa38-59342fd192f8"),
					ReceiptHandle: aws.String("15066bf8-c753-4788-813a-18e607d209f9"),
				},
			},
		}, nil)
		messageHandler.EXPECT().HandleMessage("This is a message body")
		sqsService.EXPECT().DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			ReceiptHandle: aws.String("15066bf8-c753-4788-813a-18e607d209f9"),
		}).Return(nil, awserr.New("ServiceUnavailable", "Received a HTTP 503", nil))
		testCompleted := make(chan struct{})
		errorLogger.EXPECT().
			Log(status.Error(codes.Internal, "Failed to delete message \"8dcc80c7-83ed-4d3c-aa38-59342fd192f8\": ServiceUnavailable: Received a HTTP 503")).
			Do(func(err error) { close(testCompleted) })

		require.NoError(t, sr.PerformSingleRequest())
		<-testCompleted
	})

	t.Run("Success", func(t *testing.T) {
		// The full workflow, where two messages are received,
		// handled and deleted from the queue.
		sqsService.EXPECT().ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(600),
			WaitTimeSeconds:     aws.Int64(20),
		}).Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{
				{
					Body:          aws.String("This is a message body"),
					MessageId:     aws.String("8dcc80c7-83ed-4d3c-aa38-59342fd192f8"),
					ReceiptHandle: aws.String("15066bf8-c753-4788-813a-18e607d209f9"),
				},
				{
					Body:          aws.String("This is another message body"),
					MessageId:     aws.String("aacc1ff6-b2aa-4c9a-8fc9-f2a01354f7df"),
					ReceiptHandle: aws.String("6eef738e-ca40-449b-8771-f3604ebba993"),
				},
			},
		}, nil)

		messageHandler.EXPECT().HandleMessage("This is a message body")
		testCompleted1 := make(chan struct{})
		sqsService.EXPECT().DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			ReceiptHandle: aws.String("15066bf8-c753-4788-813a-18e607d209f9"),
		}).DoAndReturn(func(in *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
			close(testCompleted1)
			return &sqs.DeleteMessageOutput{}, nil
		})

		messageHandler.EXPECT().HandleMessage("This is another message body")
		testCompleted2 := make(chan struct{})
		sqsService.EXPECT().DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String("https://sqs.eu-west-1.amazonaws.com/249843598229/MySQSQueue"),
			ReceiptHandle: aws.String("6eef738e-ca40-449b-8771-f3604ebba993"),
		}).DoAndReturn(func(in *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
			close(testCompleted2)
			return &sqs.DeleteMessageOutput{}, nil
		})

		require.NoError(t, sr.PerformSingleRequest())
		<-testCompleted1
		<-testCompleted2
	})
}
