package routing

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type simpleActionRouter struct {
	platformKeyExtractor     platform.KeyExtractor
	invocationKeyExtractors  []invocation.KeyExtractor
	initialSizeClassAnalyzer initialsizeclass.Analyzer
}

// NewSimpleActionRouter creates an ActionRouter that creates a platform
// key, invocation key and initial size class selector by independently
// calling into separate extractors/analyzers.
//
// This implementation should be sufficient for most simple setups,
// where only a small number of execution platforms exist, or where
// scheduling decisions are identical for all platforms.
func NewSimpleActionRouter(platformKeyExtractor platform.KeyExtractor, invocationKeyExtractors []invocation.KeyExtractor, initialSizeClassAnalyzer initialsizeclass.Analyzer) ActionRouter {
	return &simpleActionRouter{
		platformKeyExtractor:     platformKeyExtractor,
		invocationKeyExtractors:  invocationKeyExtractors,
		initialSizeClassAnalyzer: initialSizeClassAnalyzer,
	}
}

func (ar *simpleActionRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (platform.Key, []invocation.Key, initialsizeclass.Selector, error) {
	platformKey, err := ar.platformKeyExtractor.ExtractKey(ctx, digestFunction, action)
	if err != nil {
		return platform.Key{}, nil, nil, util.StatusWrap(err, "Failed to extract platform key")
	}
	invocationKeys := make([]invocation.Key, 0, len(ar.invocationKeyExtractors))
	for _, invocationKeyExtractor := range ar.invocationKeyExtractors {
		invocationKey, err := invocationKeyExtractor.ExtractKey(ctx, requestMetadata)
		if err != nil {
			return platform.Key{}, nil, nil, util.StatusWrap(err, "Failed to extract invocation key")
		}
		invocationKeys = append(invocationKeys, invocationKey)
	}
	initialSizeClassSelector, err := ar.initialSizeClassAnalyzer.Analyze(ctx, digestFunction, action)
	if err != nil {
		return platform.Key{}, nil, nil, util.StatusWrap(err, "Failed to analyze initial size class")
	}
	return platformKey, invocationKeys, initialSizeClassSelector, nil
}
