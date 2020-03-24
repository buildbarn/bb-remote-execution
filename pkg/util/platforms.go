package util

import (
	"sort"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type ByPlatformProperty []*remoteexecution.Platform_Property

func (a ByPlatformProperty) Len() int           { return len(a) }
func (a ByPlatformProperty) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a ByPlatformProperty) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func SortPlatformProperties(platform *remoteexecution.Platform) *remoteexecution.Platform {
	sort.Sort(ByPlatformProperty(platform.Properties))
	return platform
}
