package util_test

import (
	"fmt"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
)

func TestSortPlatformProperties(t *testing.T) {
	reverseSortedPlatform := &remoteexecution.Platform{
		Properties: []*remoteexecution.Platform_Property{
			{Name: "os", Value: "armv6"},
			{Name: "cpu", Value: "linux"},
		},
	}

	sortedPlatform := re_util.SortPlatformProperties(reverseSortedPlatform)
	if sortedPlatform.Properties[0].Name == "os" {
		t.Errorf("Platform properties did not sort correctly")
	}
}
