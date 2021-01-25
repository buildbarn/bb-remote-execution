// +build windows

package main

import (
	"os"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getSysProcAttrFromConfiguration returns a SysProcAttr object that can
// be passed to LocalRunner to run child processes with custom
// credentials.
func getSysProcAttrFromConfiguration(configuration *bb_runner.UNIXCredentialsConfiguration) (*syscall.SysProcAttr, int, error) {
	if configuration != nil {
		return nil, 0, status.Error(codes.InvalidArgument, "UNIX credentials cannot be specified on this platform")
	}
	return &syscall.SysProcAttr{}, os.Getuid(), nil
}
