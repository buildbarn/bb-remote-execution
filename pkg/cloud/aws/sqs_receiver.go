package aws

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
)

const (
	// 10 is the maximum value permitted by SQS.
	sqsMessagesPerCall = 10
)

var (
	sqsReceiverPrometheusMetrics sync.Once

	sqsReceiverReceiveFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "cloud_aws",
			Name:      "sqs_receiver_receive_failures_total",
			Help:      "Number of times SQSClient.ReceiveMessages() failed.",
		})
	sqsReceiverMessagesReceived = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "cloud_aws",
			Name:      "sqs_receiver_messages_received",
			Help:      "Number of messages returned by SQSClient.ReceiveMessages().",
			Buckets:   prometheus.LinearBuckets(0, 1, sqsMessagesPerCall+1),
		})
	sqsReceiverMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "cloud_aws",
			Name:      "sqs_receiver_messages_processed_total",
			Help:      "Number of messages processed by SQSReceiver.",
		},
		[]string{"result"})
	sqsReceiverMessagesProcessedHandlerFailure  = sqsReceiverMessagesProcessed.WithLabelValues("HandlerFailure")
	sqsReceiverMessagesProcessedDeletionFailure = sqsReceiverMessagesProcessed.WithLabelValues("DeletionFailure")
	sqsReceiverMessagesProcessedSuccess         = sqsReceiverMessagesProcessed.WithLabelValues("Success")
)

// SQSClient is an interface around the AWS SDK SQS client. It contains
// the operations that are used by SQSReceiver. It is present to aid
// unit testing.
type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

var _ SQSClient = (*sqs.Client)(nil)

// SQSMessageHandler provides a callback that is used by SQSReceiver
// that is invoked for every message that has been received. When this
// callback returns success, the message is removed from the queue.
type SQSMessageHandler interface {
	HandleMessage(body string) error
}

// SQSReceiver is a simple client for SQS that receives messages from
// SQS. Every message is provided to a handler that processes the
// message in its own goroutine. When the message is handled
// successfully, it is removed from SQS.
type SQSReceiver struct {
	sqsClient         SQSClient
	url               *string
	visibilityTimeout int32
	messageHandler    SQSMessageHandler
	errorLogger       util.ErrorLogger
}

// NewSQSReceiver creates a new SQSReceiver.
func NewSQSReceiver(sqsClient SQSClient, url string, visibilityTimeout time.Duration, messageHandler SQSMessageHandler, errorLogger util.ErrorLogger) *SQSReceiver {
	sqsReceiverPrometheusMetrics.Do(func() {
		prometheus.MustRegister(sqsReceiverReceiveFailures)
		prometheus.MustRegister(sqsReceiverMessagesReceived)
		prometheus.MustRegister(sqsReceiverMessagesProcessed)
	})

	return &SQSReceiver{
		sqsClient:         sqsClient,
		url:               aws.String(url),
		visibilityTimeout: int32(visibilityTimeout.Seconds()),
		messageHandler:    messageHandler,
		errorLogger:       errorLogger,
	}
}

// PerformSingleRequest receives a single batch of messages from SQS.
// This function generally needs to be called in a loop.
func (sr *SQSReceiver) PerformSingleRequest() error {
	ctx := context.Background()
	receivedMessageOutput, err := sr.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            sr.url,
		MaxNumberOfMessages: sqsMessagesPerCall,
		VisibilityTimeout:   sr.visibilityTimeout,
		// Documented maximum long polling wait time.
		WaitTimeSeconds: 20,
	})
	if err != nil {
		sqsReceiverReceiveFailures.Inc()
		return err
	}
	sqsReceiverMessagesReceived.Observe(float64(len(receivedMessageOutput.Messages)))

	for i := range receivedMessageOutput.Messages {
		msg := &receivedMessageOutput.Messages[i]
		go func() {
			if err := sr.messageHandler.HandleMessage(*msg.Body); err != nil {
				sqsReceiverMessagesProcessedHandlerFailure.Inc()
				sr.errorLogger.Log(util.StatusWrapf(err, "Failed to process message %#v", *msg.MessageId))
			} else if _, err := sr.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      sr.url,
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				sqsReceiverMessagesProcessedDeletionFailure.Inc()
				sr.errorLogger.Log(util.StatusWrapfWithCode(err, codes.Internal, "Failed to delete message %#v", *msg.MessageId))
			} else {
				sqsReceiverMessagesProcessedSuccess.Inc()
			}
		}()
	}
	return nil
}
