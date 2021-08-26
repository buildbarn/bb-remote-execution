package builder

import (
	"context"
	"sync"

	cal_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/completedactionlogger"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	completedActionLoggingPrometheusMetrics sync.Once

	completedActionLoggerCompletedActionsAcknowledged = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "builder",
		Name:      "completed_action_logger_completed_actions_acknowledged_total",
		Help:      "Number of Completed Actions that the remote server responded to.",
	})
	completedActionLoggerCompletedActionsLogged = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "builder",
		Name:      "completed_action_logger_completed_actions_logged_total",
		Help:      "Number of Completed Actions that were queued to be sent to a remote server or discarded.",
	},
		[]string{"result"},
	)
	completedActionLoggerCompletedActionsLoggedQueued    = completedActionLoggerCompletedActionsLogged.WithLabelValues("Queued")
	completedActionLoggerCompletedActionsLoggedDiscarded = completedActionLoggerCompletedActionsLogged.WithLabelValues("Discarded")

	completedActionLoggerCompletedActionsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "builder",
		Name:      "completed_action_logger_completed_actions_sent_total",
		Help:      "Number of Completed Actions sent to a remote service.",
	})
)

// The CompletedActionLogger can be used to record CompletedActions for
// realtime or post-build analysis in a remote service. This is particularly
// useful for understanding how build actions change over time by inspecting
// the aggregated CompletedAction metadata.
type CompletedActionLogger interface {
	LogCompletedAction(completedAction *cal_proto.CompletedAction)
}

// The RemoteCompletedActionLogger type is used to store and send
// CompletedActions for a completedActionLoggingBuildExecutor. It keeps track
// of which messages have been previously transmitted and retries them if our
// connection with the server is interrupted.
type RemoteCompletedActionLogger struct {
	client               cal_proto.CompletedActionLoggerClient
	maximumSendQueueSize int

	lock       sync.Mutex
	SendQueue  []*cal_proto.CompletedAction
	sendWakeup chan struct{}
}

// NewRemoteCompletedActionLogger returns a new RemoteCompletedActionLogger
// with a predefined maximum capacity of stored messages. This ensures that
// we don't overwhelm the server in case it is under heavy load
// and cannot respond.
func NewRemoteCompletedActionLogger(queueSize int, client cal_proto.CompletedActionLoggerClient) *RemoteCompletedActionLogger {
	completedActionLoggingPrometheusMetrics.Do(func() {
		prometheus.MustRegister(completedActionLoggerCompletedActionsAcknowledged)
		prometheus.MustRegister(completedActionLoggerCompletedActionsLogged)
		prometheus.MustRegister(completedActionLoggerCompletedActionsSent)
	})

	return &RemoteCompletedActionLogger{
		client:               client,
		maximumSendQueueSize: queueSize,

		lock:       sync.Mutex{},
		SendQueue:  []*cal_proto.CompletedAction{},
		sendWakeup: make(chan struct{}, queueSize),
	}
}

// LogCompletedAction will add one CompletedAction to the
// RemoteCompletedActionLogger and notify that a message
// is ready to be transmitted.
func (logger *RemoteCompletedActionLogger) LogCompletedAction(completedAction *cal_proto.CompletedAction) {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	if len(logger.SendQueue) < logger.maximumSendQueueSize {
		logger.SendQueue = append(logger.SendQueue, completedAction)
		close(logger.sendWakeup)
		logger.sendWakeup = make(chan struct{})
		completedActionLoggerCompletedActionsLoggedQueued.Inc()
	} else {
		completedActionLoggerCompletedActionsLoggedDiscarded.Inc()
	}
}

// SendAllCompletedActions is responsible for managing goroutines that perform
// the rpc transmission and response handling.
func (logger *RemoteCompletedActionLogger) SendAllCompletedActions() error {
	eg, ctx := errgroup.WithContext(context.Background())
	stream, err := logger.client.LogCompletedActions(ctx)
	if err != nil {
		return err
	}

	actionsSent := 0

	eg.Go(func() error {
		for {
			logger.lock.Lock()
			c := logger.sendWakeup

			actionsToSend := logger.SendQueue[actionsSent:]
			actionsSent = len(logger.SendQueue)
			logger.lock.Unlock()

			for _, action := range actionsToSend {
				if err := stream.Send(action); err != nil {
					return util.StatusWrapf(err, "Failed to transmit completed action %#v", action.Uuid)
				}
				completedActionLoggerCompletedActionsSent.Inc()
			}
			select {
			case <-c:
			case <-ctx.Done():
				return util.StatusFromContext(ctx)
			}
		}
	})
	eg.Go(func() error {
		for {
			if _, err := stream.Recv(); err != nil {
				return util.StatusWrap(err, "Failed to receive response from server")
			}

			logger.lock.Lock()
			if actionsSent == 0 {
				logger.lock.Unlock()
				return status.Error(codes.FailedPrecondition, "Improper response: No messages left in the queue")
			}

			logger.SendQueue = logger.SendQueue[1:]
			actionsSent--
			logger.lock.Unlock()
			completedActionLoggerCompletedActionsAcknowledged.Inc()
		}
	})
	err = eg.Wait()

	// Ensure we close the stream properly by calling Recv
	// until we get an error response.
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	return err
}
