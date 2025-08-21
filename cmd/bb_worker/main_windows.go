//go:build windows
// +build windows

package main

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getInputRootCharacterDevices(names []string) (map[path.Component]filesystem.DeviceNumber, error) {
	if len(names) > 0 {
		return nil, status.Error(codes.Unimplemented, "Character devices are not supported on this platform")
	}
	return map[path.Component]filesystem.DeviceNumber{}, nil
}

// On Windows, we make all build actions execute inside a directory that
// ends with execroot/_main in order to workaround a Bazel bug with MSVC
// include checking: https://github.com/bazelbuild/bazel/issues/19733.
// Without this, all compilation with MSVC would fail with disallowed
// inclusions. Note this only fixes bzlmod builds; there is no known
// workaround for workspace builds.
var inputRootComponents = []path.Component{path.MustNewComponent("execroot"), path.MustNewComponent("_main")}
