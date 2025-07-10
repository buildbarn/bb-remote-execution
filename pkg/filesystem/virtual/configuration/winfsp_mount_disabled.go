//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package configuration

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/program"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *winfspMount) Expose(terminationGroup program.Group, rootDirectory virtual.Directory) error {
	return status.Error(codes.Unimplemented, "WinFSP is not supported on this platform")
}
