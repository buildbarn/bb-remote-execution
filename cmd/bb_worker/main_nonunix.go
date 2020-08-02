// +build windows

package main

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func clearUmask() {
}

func getInputRootCharacterDevices(names []string) (map[string]int, error) {
	if len(names) > 0 {
		return nil, status.Error(codes.Unimplemented, "Character devices are not supported on this platform")
	}
	return map[string]int{}, nil
}
