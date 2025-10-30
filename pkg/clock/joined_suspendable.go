package clock

type joinedSuspendable struct {
	suspendables []Suspendable
}

// NewJoinedSuspendable merges a list of Suspendables into a single
// instance. Any attempts to suspend or resume will be forwarded to all
// underlying instances.
//
// This can be used by the HTTP execution timeout compensator, which is
// configured for an entire worker, as opposed to a single worker
// thread.
func NewJoinedSuspendable(suspendables []Suspendable) Suspendable {
	return joinedSuspendable{
		suspendables: suspendables,
	}
}

func (s joinedSuspendable) Suspend() {
	for _, suspendable := range s.suspendables {
		suspendable.Suspend()
	}
}

func (s joinedSuspendable) Resume() {
	for _, suspendable := range s.suspendables {
		suspendable.Resume()
	}
}
