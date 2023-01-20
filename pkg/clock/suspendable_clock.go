package clock

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
)

// Suspendable is an interface type of the methods of SuspendableClock
// that are used by NewSuspendingBlobAccess() and
// NewSuspendingDirectoryFetcher().
type Suspendable interface {
	Suspend()
	Resume()
}

// UnsuspendedDurationKey instances can be provided to Context.Value()
// to obtain the total amount of time a SuspendableClock associated with
// the Context object was not suspended, since the creation of the
// Context.
type UnsuspendedDurationKey struct{}

// SuspendableClock is a decorator for Clock that allows Timers and
// Contexts with timeouts to be suspended temporarily. This decorator
// can, for example, be used to let FUSE-based workers compensate the
// execution timeout of build actions with time spent performing
// BlobAccess reads.
type SuspendableClock struct {
	base              clock.Clock
	maximumSuspension time.Duration
	timeoutThreshold  time.Duration

	lock              sync.Mutex
	suspensionCount   int
	unsuspensionStart time.Time
	totalUnsuspended  time.Duration
}

var (
	_ clock.Clock = (*SuspendableClock)(nil)
	_ Suspendable = (*SuspendableClock)(nil)
)

// NewSuspendableClock creates a new SuspendableClock.
//
// The maximumSuspension argument denotes the maximum amount of time
// Timers and Contexts with timeouts may be suspended. This prevents
// users from bypassing timeouts when creating access patterns that
// cause the clock to be suspended excessively.
func NewSuspendableClock(base clock.Clock, maximumSuspension, timeoutThreshold time.Duration) *SuspendableClock {
	return &SuspendableClock{
		base:              base,
		maximumSuspension: maximumSuspension,
		timeoutThreshold:  timeoutThreshold,
		unsuspensionStart: time.Unix(0, 0),
	}
}

// Suspend the Clock, thereby causing all active Timer and Context
// objects to not trigger (except if the maximum suspension duration is
// reached).
//
// Clocks can be suspended multiple times. The clock will only continue
// to run if Resume() is called an equal number of times.
func (c *SuspendableClock) Suspend() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.suspensionCount == 0 {
		c.totalUnsuspended += c.base.Now().Sub(c.unsuspensionStart)
	}
	c.suspensionCount++
}

// Resume the Clock, thereby causing all active Timer and Context
// objects to continue processing.
func (c *SuspendableClock) Resume() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.suspensionCount == 0 {
		panic("Attempted to release a suspendedable clock that wasn't acquired")
	}
	c.suspensionCount--
	if c.suspensionCount == 0 {
		c.unsuspensionStart = c.base.Now()
	}
}

// Now returns the current time of day.
func (c *SuspendableClock) Now() time.Time {
	return c.base.Now()
}

// getTotalUnsuspendedNow computes the total amount of time the clock
// was not suspended since its creation. If the current time is needed
// to compute the exact duration, a call is made into the underlying
// clock.
func (c *SuspendableClock) getTotalUnsuspendedNow() time.Duration {
	totalUnsuspended := c.totalUnsuspended
	if c.suspensionCount == 0 {
		// Clock is not suspended right now.
		totalUnsuspended += c.base.Now().Sub(c.unsuspensionStart)
	}
	return totalUnsuspended
}

// getTotalUnsuspendedWithTime is identical to getTotalUnsuspendedNow,
// except that a known time value can be provided.
func (c *SuspendableClock) getTotalUnsuspendedWithTime(now time.Time) time.Duration {
	totalUnsuspended := c.totalUnsuspended
	if c.suspensionCount == 0 && now.After(c.unsuspensionStart) {
		// Clock is not suspended right now.
		totalUnsuspended += now.Sub(c.unsuspensionStart)
	}
	return totalUnsuspended
}

// NewContextWithTimeout creates a Context object that automatically
// cancels itself after a certain amount of time has passed, taking
// suspensions of the Clock into account.
func (c *SuspendableClock) NewContextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	// Wrap the context into one that the maximum suspension already
	// applied. This ensures that Context.Deadline() returns a
	// proper upper bound. It also provides us a usable CancelFunc.
	baseContext, baseCancel := c.base.NewContextWithTimeout(parent, d+c.maximumSuspension)
	baseDoneChannel := baseContext.Done()

	// Create a context object to be returned by this function.
	doneChannel := make(chan struct{})
	ctx := &suspendableContext{
		Context:     baseContext,
		doneChannel: doneChannel,

		lock: &c.lock,
	}

	// Gather the initial amount of time not suspended at the start,
	// so that we can compute the total amount we've observed upon
	// completion.
	c.lock.Lock()
	go func() {
		initialTotalUnsuspended := c.getTotalUnsuspendedNow()
		finalTotalUnsuspended := initialTotalUnsuspended + d
		for {
			c.lock.Unlock()
			baseTimer, baseChannel := c.base.NewTimer(d)
			select {
			case now := <-baseChannel:
				// Timer expired.
				c.lock.Lock()
				currentTotalUnsuspended := c.getTotalUnsuspendedWithTime(now)
				d = finalTotalUnsuspended - currentTotalUnsuspended
				if d < c.timeoutThreshold {
					// Amount of time suspended in
					// the meantime is not worth
					// creating another timer for.
					ctx.err = context.DeadlineExceeded
					ctx.unsuspendedDuration = currentTotalUnsuspended - initialTotalUnsuspended
					c.lock.Unlock()
					close(doneChannel)
					return
				}
			case <-baseDoneChannel:
				// Base context got canceled.
				c.lock.Lock()
				ctx.err = baseContext.Err()
				ctx.unsuspendedDuration = c.getTotalUnsuspendedNow() - initialTotalUnsuspended
				c.lock.Unlock()
				baseTimer.Stop()
				close(doneChannel)
				return
			}
		}
	}()

	return ctx, baseCancel
}

// NewTimer creates a channel that publishes the time of day at a point
// of time in the future, taking suspension of the Clock into account.
func (c *SuspendableClock) NewTimer(d time.Duration) (clock.Timer, <-chan time.Time) {
	// Create a timer that puts an upper bound on the amount of time
	// the timer may be delayed due to suspension. This prevents
	// users from delaying the timer indefinitely.
	maximumSuspensionTimer, maximumSuspensionChannel := c.base.NewTimer(d + c.maximumSuspension)

	// Create timer handle to be returned by this function.
	stopChannel := make(chan struct{})
	t := &suspendableTimer{
		lock:        &c.lock,
		stopChannel: stopChannel,
	}
	resultChannel := make(chan time.Time, 1)

	c.lock.Lock()
	go func() {
		finalTotalUnsuspended := c.getTotalUnsuspendedNow() + d
		for {
			c.lock.Unlock()
			baseTimer, baseChannel := c.base.NewTimer(d)
			select {
			case now := <-baseChannel:
				// Timer expired.
				c.lock.Lock()
				d = finalTotalUnsuspended - c.getTotalUnsuspendedWithTime(now)
				if d < c.timeoutThreshold {
					// Amount of time suspended in
					// the meantime is not worth
					// creating another timer for.
					t.stopChannel = nil
					c.lock.Unlock()
					maximumSuspensionTimer.Stop()
					resultChannel <- now
					return
				}
			case now := <-maximumSuspensionChannel:
				// Maximum amount of suspension reached.
				c.lock.Lock()
				t.stopChannel = nil
				c.lock.Unlock()
				baseTimer.Stop()
				resultChannel <- now
				return
			case <-stopChannel:
				// Timer stopped.
				maximumSuspensionTimer.Stop()
				baseTimer.Stop()
				return
			}
		}
	}()
	return t, resultChannel
}

// suspendableContext is the implementation of Context that is returned
// by SuspendableClock.NewContextWithTimeout().
type suspendableContext struct {
	context.Context
	doneChannel <-chan struct{}

	lock                *sync.Mutex
	err                 error
	unsuspendedDuration time.Duration
}

func (ctx *suspendableContext) Done() <-chan struct{} {
	return ctx.doneChannel
}

func (ctx *suspendableContext) Err() error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	return ctx.err
}

func (ctx *suspendableContext) Value(key interface{}) interface{} {
	if key != (UnsuspendedDurationKey{}) {
		return ctx.Context.Value(key)
	}

	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	return ctx.unsuspendedDuration
}

// suspendableTimer is the implementation of Timer that is returned by
// SuspendableClock.NewTimer(). Its sole purpose is to provide
// cancelation.
type suspendableTimer struct {
	lock        *sync.Mutex
	stopChannel chan<- struct{}
}

func (ctx *suspendableTimer) Stop() bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	if ctx.stopChannel != nil {
		close(ctx.stopChannel)
		ctx.stopChannel = nil
		return true
	}
	return false
}
