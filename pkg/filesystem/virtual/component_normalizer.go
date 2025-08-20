package virtual

import (
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// NormalizedComponent is a normalized version of a path.Component.
// The meaning of normalized depends on what ComponentNormalizer was used to
// normalize the path.
type NormalizedComponent string

// ComponentNormalizer is an interface for implementing normalization of
// path.Component.
type ComponentNormalizer interface {
	Normalize(path.Component) NormalizedComponent
}

type caseSensitiveComponentNormalizer struct{}

func (caseSensitiveComponentNormalizer) Normalize(c path.Component) NormalizedComponent {
	return NormalizedComponent(c.String())
}

// CaseSensitiveComponentNormalizer is a ComponentNormalizer that normalizes
// path components in a case-sensitive manner.
var CaseSensitiveComponentNormalizer ComponentNormalizer = caseSensitiveComponentNormalizer{}

type caseInsensitiveComponentNormalizer struct{}

func (caseInsensitiveComponentNormalizer) Normalize(c path.Component) NormalizedComponent {
	return NormalizedComponent(strings.ToLower(c.String()))
}

// CaseInsensitiveComponentNormalizer is a ComponentNormalizer that normalizes
// path components in a case-insensitive manner.
var CaseInsensitiveComponentNormalizer ComponentNormalizer = caseInsensitiveComponentNormalizer{}
