package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestSuspendableClockNow(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseClock := mock.NewMockClock(ctrl)
	suspendableClock := clock.NewSuspendableClock(baseClock, time.Hour, time.Second)

	t.Run("Success", func(t *testing.T) {
		// Calls to obtain the time of day should simply be forwarded.
		baseClock.EXPECT().Now().Return(time.Unix(123, 0))
		require.Equal(t, time.Unix(123, 0), suspendableClock.Now())
	})
}

func TestSuspendableClockNewContextWithTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseClock := mock.NewMockClock(ctrl)
	suspendableClock := clock.NewSuspendableClock(baseClock, time.Hour, time.Second)

	t.Run("NoSuspension", func(t *testing.T) {
		// When no suspension calls happen against the
		// SuspendableClock, the context should time out after
		// the provided amount of time.
		baseContext := mock.NewMockContext(ctrl)
		maximumSuspensionContext := mock.NewMockContext(ctrl)
		maximumSuspensionCancel := mock.NewMockCancelFunc(ctrl)
		baseClock.EXPECT().NewContextWithTimeout(baseContext, time.Hour+5*time.Second).
			Return(maximumSuspensionContext, maximumSuspensionCancel.Call)
		baseClock.EXPECT().Now().Return(time.Unix(1018, 0))
		maximumSuspensionDoneChannel := make(chan struct{})
		maximumSuspensionContext.EXPECT().Done().Return(maximumSuspensionDoneChannel)
		baseTimer := mock.NewMockTimer(ctrl)
		baseChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer, baseChannel)

		suspendableContext, suspendableCancel := suspendableClock.NewContextWithTimeout(baseContext, 5*time.Second)
		suspendableDoneChannel := suspendableContext.Done()
		require.Empty(t, suspendableDoneChannel)

		baseChannel <- time.Unix(1023, 0)

		<-suspendableContext.Done()
		require.Equal(t, context.DeadlineExceeded, suspendableContext.Err())

		maximumSuspensionCancel.EXPECT().Call()
		suspendableCancel()

		require.Equal(t, 5*time.Second, suspendableContext.Value(clock.UnsuspendedDurationKey{}))
	})

	t.Run("Canceled", func(t *testing.T) {
		// Cancellation of the suspendable context should cause
		// all associated resources to be freed.
		baseContext := mock.NewMockContext(ctrl)
		maximumSuspensionContext := mock.NewMockContext(ctrl)
		maximumSuspensionCancel := mock.NewMockCancelFunc(ctrl)
		baseClock.EXPECT().NewContextWithTimeout(baseContext, time.Hour+5*time.Second).
			Return(maximumSuspensionContext, maximumSuspensionCancel.Call)
		baseClock.EXPECT().Now().Return(time.Unix(1100, 0))
		maximumSuspensionDoneChannel := make(chan struct{})
		maximumSuspensionContext.EXPECT().Done().Return(maximumSuspensionDoneChannel)
		baseTimer := mock.NewMockTimer(ctrl)
		baseChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer, baseChannel)

		suspendableContext, suspendableCancel := suspendableClock.NewContextWithTimeout(baseContext, 5*time.Second)
		suspendableDoneChannel := suspendableContext.Done()
		require.Empty(t, suspendableDoneChannel)

		baseClock.EXPECT().Now().Return(time.Unix(1103, 0))
		maximumSuspensionCancel.EXPECT().Call()
		maximumSuspensionContext.EXPECT().Err().Return(context.Canceled)
		baseTimer.EXPECT().Stop().Return(true)

		suspendableCancel()

		close(maximumSuspensionDoneChannel)

		<-suspendableContext.Done()
		require.Equal(t, context.Canceled, suspendableContext.Err())

		require.Equal(t, 3*time.Second, suspendableContext.Value(clock.UnsuspendedDurationKey{}))
	})

	t.Run("Suspension", func(t *testing.T) {
		// Create a context with a timeout of five seconds. Suspend
		// the clock for one seconds during these five seconds.
		// This should cause a second timer to be created with a
		// one second timeout.
		baseContext := mock.NewMockContext(ctrl)
		maximumSuspensionContext := mock.NewMockContext(ctrl)
		maximumSuspensionCancel := mock.NewMockCancelFunc(ctrl)
		baseClock.EXPECT().NewContextWithTimeout(baseContext, time.Hour+5*time.Second).
			Return(maximumSuspensionContext, maximumSuspensionCancel.Call)
		baseClock.EXPECT().Now().Return(time.Unix(1220, 0))
		maximumSuspensionDoneChannel := make(chan struct{})
		maximumSuspensionContext.EXPECT().Done().Return(maximumSuspensionDoneChannel)
		baseTimer1 := mock.NewMockTimer(ctrl)
		baseChannel1 := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer1, baseChannel1)

		suspendableContext, suspendableCancel := suspendableClock.NewContextWithTimeout(baseContext, 5*time.Second)
		suspendableDoneChannel := suspendableContext.Done()
		require.Empty(t, suspendableDoneChannel)

		baseClock.EXPECT().Now().Return(time.Unix(1222, 0))
		suspendableClock.Suspend()

		// It is possible to suspend the clock recursively. It
		// should have no effect on the bookkeeping.
		suspendableClock.Suspend()
		suspendableClock.Suspend()
		suspendableClock.Resume()
		suspendableClock.Resume()

		baseClock.EXPECT().Now().Return(time.Unix(1223, 0))
		suspendableClock.Resume()

		baseTimer2 := mock.NewMockTimer(ctrl)
		baseChannel2 := make(chan time.Time)
		baseClock.EXPECT().NewTimer(1*time.Second).Return(baseTimer2, baseChannel2)

		baseChannel1 <- time.Unix(1225, 0)

		baseChannel2 <- time.Unix(1226, 0)

		<-suspendableContext.Done()
		require.Equal(t, context.DeadlineExceeded, suspendableContext.Err())

		maximumSuspensionCancel.EXPECT().Call()
		suspendableCancel()

		require.Equal(t, 5*time.Second, suspendableContext.Value(clock.UnsuspendedDurationKey{}))
	})
}

func TestSuspendableClockNewTimer(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseClock := mock.NewMockClock(ctrl)
	suspendableClock := clock.NewSuspendableClock(baseClock, time.Hour, time.Second)

	t.Run("NoSuspension", func(t *testing.T) {
		// When no suspension calls happen against the
		// SuspendableClock, a timer created from it should
		// behave like an ordinary one.
		maximumSuspensionTimer := mock.NewMockTimer(ctrl)
		maximumSuspensionChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(time.Hour+5*time.Second).Return(maximumSuspensionTimer, maximumSuspensionChannel)
		baseClock.EXPECT().Now().Return(time.Unix(1018, 0))
		baseTimer := mock.NewMockTimer(ctrl)
		baseChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer, baseChannel)

		suspendableTimer, suspendableChannel := suspendableClock.NewTimer(5 * time.Second)
		require.Empty(t, suspendableChannel)

		maximumSuspensionTimer.EXPECT().Stop().Return(true)

		baseChannel <- time.Unix(1023, 0)
		require.Equal(t, time.Unix(1023, 0), <-suspendableChannel)

		require.False(t, suspendableTimer.Stop())
	})

	t.Run("Stopped", func(t *testing.T) {
		// Cancellations on the timer returned by
		// SuspendableClock should be propagated to the
		// underlying instance.
		maximumSuspensionTimer := mock.NewMockTimer(ctrl)
		maximumSuspensionChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(time.Hour+5*time.Second).Return(maximumSuspensionTimer, maximumSuspensionChannel)
		baseClock.EXPECT().Now().Return(time.Unix(1105, 0))
		baseTimer := mock.NewMockTimer(ctrl)
		baseChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer, baseChannel)

		suspendableTimer, suspendableChannel := suspendableClock.NewTimer(5 * time.Second)
		require.Empty(t, suspendableChannel)

		maximumSuspensionTimer.EXPECT().Stop().Return(true)
		wait := make(chan struct{})
		baseTimer.EXPECT().Stop().DoAndReturn(func() bool {
			close(wait)
			return true
		})
		require.True(t, suspendableTimer.Stop())
		<-wait
	})

	t.Run("Suspension", func(t *testing.T) {
		// Create a timer that runs for five seconds. Suspend
		// the clock for one seconds during these five seconds.
		// This should cause a second timer to be created with a
		// one second timeout.
		maximumSuspensionTimer := mock.NewMockTimer(ctrl)
		maximumSuspensionChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(time.Hour+5*time.Second).Return(maximumSuspensionTimer, maximumSuspensionChannel)
		baseClock.EXPECT().Now().Return(time.Unix(1220, 0))
		baseTimer1 := mock.NewMockTimer(ctrl)
		baseChannel1 := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer1, baseChannel1)

		_, suspendableChannel := suspendableClock.NewTimer(5 * time.Second)
		require.Empty(t, suspendableChannel)

		baseClock.EXPECT().Now().Return(time.Unix(1222, 0))
		suspendableClock.Suspend()

		// It is possible to suspend the clock recursively. It
		// should have no effect on the bookkeeping.
		suspendableClock.Suspend()
		suspendableClock.Suspend()
		suspendableClock.Resume()
		suspendableClock.Resume()

		baseClock.EXPECT().Now().Return(time.Unix(1223, 0))
		suspendableClock.Resume()

		baseTimer2 := mock.NewMockTimer(ctrl)
		baseChannel2 := make(chan time.Time)
		baseClock.EXPECT().NewTimer(1*time.Second).Return(baseTimer2, baseChannel2)

		baseChannel1 <- time.Unix(1225, 0)

		maximumSuspensionTimer.EXPECT().Stop().Return(true)

		baseChannel2 <- time.Unix(1226, 0)
		require.Equal(t, time.Unix(1226, 0), <-suspendableChannel)
	})

	t.Run("SuspensionTooSmall", func(t *testing.T) {
		// Suspend the clock for just a very small amount of
		// time. This should not cause a second timer to be
		// created, as that would only contribute to more load
		// on the system.
		maximumSuspensionTimer := mock.NewMockTimer(ctrl)
		maximumSuspensionChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(time.Hour+5*time.Second).Return(maximumSuspensionTimer, maximumSuspensionChannel)
		baseClock.EXPECT().Now().Return(time.Unix(1320, 0))
		baseTimer := mock.NewMockTimer(ctrl)
		baseChannel := make(chan time.Time)
		baseClock.EXPECT().NewTimer(5*time.Second).Return(baseTimer, baseChannel)

		_, suspendableChannel := suspendableClock.NewTimer(5 * time.Second)
		require.Empty(t, suspendableChannel)

		baseClock.EXPECT().Now().Return(time.Unix(1322, 0))
		suspendableClock.Suspend()

		baseClock.EXPECT().Now().Return(time.Unix(1322, 500000000))
		suspendableClock.Resume()

		maximumSuspensionTimer.EXPECT().Stop().Return(true)

		baseChannel <- time.Unix(1325, 0)
		require.Equal(t, time.Unix(1325, 0), <-suspendableChannel)
	})
}
