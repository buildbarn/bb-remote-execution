//go:build darwin || freebsd || windows
// +build darwin freebsd windows

package configuration

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/program"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *fuseMount) Expose(terminationGroup program.Group, rootDirectory virtual.Directory) error {
	return status.Error(codes.Unimplemented, "FUSE is not supported on this platform")
}
