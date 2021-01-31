// +build darwin freebsd linux

package credentials

import (
	"os"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/credentials"
)

// GetSysProcAttrFromConfiguration returns a SysProcAttr object that can
// be passed to LocalRunner to run child processes with custom
// credentials.
func GetSysProcAttrFromConfiguration(configuration *credentials.UNIXCredentialsConfiguration) (*syscall.SysProcAttr, int, error) {
	if configuration == nil {
		return &syscall.SysProcAttr{}, os.Getuid(), nil
	}
	return &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    configuration.UserId,
			Gid:    configuration.GroupId,
			Groups: configuration.AdditionalGroupIds,
		},
	}, int(configuration.UserId), nil
}
