//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package main

import (
	"path/filepath"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getInputRootCharacterDevices(names []string) (map[path.Component]filesystem.DeviceNumber, error) {
	inputRootCharacterDevices := map[path.Component]filesystem.DeviceNumber{}
	for _, device := range names {
		var stat unix.Stat_t
		devicePath := filepath.Join("/dev", device)
		if err := unix.Stat(devicePath, &stat); err != nil {
			return nil, util.StatusWrapf(err, "Unable to stat character device %#v", devicePath)
		}
		if stat.Mode&syscall.S_IFMT != syscall.S_IFCHR {
			return nil, status.Errorf(codes.InvalidArgument, "The specified device %#v is not a character device", devicePath)
		}
		component, ok := path.NewComponent(device)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Device %#v has an invalid name", devicePath)
		}
		inputRootCharacterDevices[component] = filesystem.NewDeviceNumberFromRaw(uint64(stat.Rdev))
	}
	return inputRootCharacterDevices, nil
}

var inputRootComponents = []path.Component{path.MustNewComponent("root")}
