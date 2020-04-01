package sync

import (
	"sync"
)

// Initializer is similar to a sync.Once, except that it may go back to
// its initial state based on a use count. Initializer may be used to
// initialize and/or clean resources whenever they start being used.
// Examples may include temporary directories that need periodic
// cleaning.
type Initializer struct {
	lock  sync.Mutex
	count uint
}

// InitializationFunc is the callback that is used by
// Initializer.Acquire() to initialize a resource.
type InitializationFunc func() error

// Acquire the initializer by increasing its use count. The provided
// initialization function is called when the use count is incremented
// from zero to one.
func (i *Initializer) Acquire(init InitializationFunc) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.count == 0 {
		if err := init(); err != nil {
			return err
		}
	}
	i.count++
	return nil
}

// Release the initializer by lowering its use count.
func (i *Initializer) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.count == 0 {
		panic("Called Release() on Initializer with a zero use count")
	}
	i.count--
}
