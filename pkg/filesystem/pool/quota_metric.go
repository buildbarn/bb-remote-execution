package pool

import "sync/atomic"

// quotaMetric is a simple 64-bit counter from/to which can be
// subtracted/added atomically. It is used to store the number of files
// and bytes of space available.
type quotaMetric struct {
	remaining atomic.Int64
}

func newQuotaMetric(limit int64) *quotaMetric {
	m := &quotaMetric{
		remaining: atomic.Int64{},
	}
	m.remaining.Store(limit)
	return m
}

func (m *quotaMetric) allocate(v int64) bool {
	for {
		remaining := m.remaining.Load()
		if remaining < v {
			return false
		}
		if m.remaining.CompareAndSwap(remaining, remaining-v) {
			return true
		}
	}
}

func (m *quotaMetric) release(v int64) {
	m.remaining.Add(v)
}
