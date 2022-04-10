//go:build freebsd || windows
// +build freebsd windows

package configuration

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *fuseMount) Expose(rootDirectory virtual.Directory) error {
	return status.Error(codes.Unimplemented, "FUSE is not supported on this platform")
}
