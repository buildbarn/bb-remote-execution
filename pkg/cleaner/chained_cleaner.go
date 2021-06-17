package cleaner

import (
	"context"
)

// NewChainedCleaner creates a new Cleaner that invokes a series of
// existing Cleaner objects sequentially. If any of them fail, the first
// observed error is returned.
func NewChainedCleaner(cleaners []Cleaner) Cleaner {
	return func(ctx context.Context) error {
		var chainedErr error
		for _, cleaner := range cleaners {
			if err := cleaner(ctx); chainedErr == nil {
				chainedErr = err
			}
		}
		return chainedErr
	}
}
