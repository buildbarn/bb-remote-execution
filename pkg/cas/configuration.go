package cas

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
)

// NewCachingFetcherFromConfiguration creates a new CachingFetcher based
// on parameters provided in a configuration file.
func NewCachingFetcherFromConfiguration[T any](configuration *pb.CachingFetcherConfiguration, base Fetcher[T], name string) (Fetcher[T], error) {
	if configuration == nil {
		// No configuration provided. Disable in-memory caching.
		return base, nil
	}

	evictionSet, err := eviction.NewSetFromConfiguration(configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, err
	}
	return NewCachingFetcher(
		base,
		digest.KeyWithoutInstance,
		int(configuration.MaximumCount),
		configuration.MaximumSizeBytes,
		eviction.NewMetricsSet(evictionSet, name),
	), nil
}
