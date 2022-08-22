package cas

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
)

// NewCachingDirectoryFetcherFromConfiguration creates a new
// CachingDirectoryFetcher based on parameters provided in a
// configuration file.
func NewCachingDirectoryFetcherFromConfiguration(configuration *pb.CachingDirectoryFetcherConfiguration, base DirectoryFetcher) (DirectoryFetcher, error) {
	if configuration == nil {
		// No configuration provided. Disable in-memory caching.
		return base, nil
	}

	evictionSet, err := eviction.NewSetFromConfiguration[CachingDirectoryFetcherKey](configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, err
	}
	return NewCachingDirectoryFetcher(
		base,
		digest.KeyWithoutInstance,
		int(configuration.MaximumCount),
		configuration.MaximumSizeBytes,
		eviction.NewMetricsSet(evictionSet, "CachingDirectoryFetcher"),
	), nil
}
