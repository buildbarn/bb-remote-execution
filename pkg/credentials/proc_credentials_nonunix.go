// +build windows

package credentials

import (
	"os"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/credentials"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetSysProcAttrFromConfiguration returns a SysProcAttr object that can
// be passed to LocalRunner to run child processes with custom
// credentials.
func GetSysProcAttrFromConfiguration(configuration *credentials.UNIXCredentialsConfiguration) (*syscall.SysProcAttr, int, error) {
	if configuration != nil {
		return nil, 0, status.Error(codes.InvalidArgument, "UNIX credentials cannot be specified on this platform")
	}
	return &syscall.SysProcAttr{}, os.Getuid(), nil
}
