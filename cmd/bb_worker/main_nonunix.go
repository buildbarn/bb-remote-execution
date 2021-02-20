// +build windows

package main

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getInputRootCharacterDevices(names []string) (map[path.Component]int, error) {
	if len(names) > 0 {
		return nil, status.Error(codes.Unimplemented, "Character devices are not supported on this platform")
	}
	return map[path.Component]int{}, nil
}
