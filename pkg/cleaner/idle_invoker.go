package cleaner

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// Cleaner is a function that cleans up some resource provided by the
// operating system, such as stale processes in a process table or files
// in a temporary directory.
type Cleaner func(ctx context.Context) error

// IdleInvoker is a helper type for invoking a Cleaner function. As it's
// generally not safe to call a Cleaner function while one or more build
// actions are running, they should only be invoked when the system is
// idle.
//
// IdleInvoker keeps track of a use count to determine whether the
// system is idle. When transitioning from idle to busy or from busy to
// idle, the Cleaner function is called.
type IdleInvoker struct {
	f Cleaner

	lock     sync.Mutex
	useCount uint
	wakeup   <-chan struct{}
}

// NewIdleInvoker creates a new IdleInvoker that is in the idle state.
func NewIdleInvoker(f Cleaner) *IdleInvoker {
	return &IdleInvoker{f: f}
}

func (i *IdleInvoker) clean(ctx context.Context) error {
	if i.wakeup != nil {
		panic("Cleaning is already in progress")
	}
	wakeup := make(chan struct{})
	i.wakeup = wakeup

	i.lock.Unlock()
	err := i.f(ctx)
	i.lock.Lock()

	close(wakeup)
	i.wakeup = nil
	return err
}

// Acquire the IdleInvoker by incrementing its use count. If the use
// count transitions from zero to one, the Cleaner is called.
// Acquisition does not take place if the Cleaner returns an error.
func (i *IdleInvoker) Acquire(ctx context.Context) error {
	// Wait for existing calls to the Cleaner to finish.
	i.lock.Lock()
	for i.wakeup != nil {
		wakeup := i.wakeup
		i.lock.Unlock()
		select {
		case <-wakeup:
		case <-ctx.Done():
			return util.StatusFromContext(ctx)
		}
		i.lock.Lock()
	}
	defer i.lock.Unlock()

	// Don't perform cleaning in case we've already been acquired,
	// as we don't want to cause disruptions.
	if i.useCount == 0 {
		if err := i.clean(ctx); err != nil {
			return err
		}
	}
	i.useCount++
	return nil
}

// Release the IdleInvoker by decrementing its use count. If the use
// count transitions from one to zero, the Cleaner is called. The use
// count is always decremented, even if cleaning fails.
func (i *IdleInvoker) Release(ctx context.Context) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.useCount == 0 {
		panic("Called Release() on IdleInvoker with a zero use count")
	}
	i.useCount--
	if i.useCount > 0 {
		return nil
	}
	return i.clean(ctx)
}
