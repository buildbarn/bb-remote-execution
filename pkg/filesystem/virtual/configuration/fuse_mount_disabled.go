//go:build freebsd || windows
// +build freebsd windows

package configuration

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *fuseMount) Expose(terminationContext context.Context, terminationGroup *errgroup.Group, rootDirectory virtual.Directory) error {
	return status.Error(codes.Unimplemented, "FUSE is not supported on this platform")
}
