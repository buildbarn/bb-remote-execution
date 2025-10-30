package clock

// Suspendable is an interface type of the methods of SuspendableClock
// that are used by NewSuspendingBlobAccess() and
// NewSuspendingDirectoryFetcher().
type Suspendable interface {
	Suspend()
	Resume()
}
