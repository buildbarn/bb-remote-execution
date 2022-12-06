//go:build freebsd || linux || windows
// +build freebsd linux windows

package configuration

import (
	"context"

	"github.com/buildbarn/go-xdr/pkg/rpcserver"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *nfsv4Mount) mount(terminationContext context.Context, terminationGroup *errgroup.Group, rpcServer *rpcserver.Server) error {
	return status.Error(codes.Unimplemented, "NFSv4 is not supported on this platform")
}
