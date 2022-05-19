//go:build freebsd || linux || windows
// +build freebsd linux windows

package configuration

import (
	"github.com/buildbarn/go-xdr/pkg/rpcserver"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *nfsv4Mount) mount(rpcServer *rpcserver.Server) error {
	return status.Error(codes.Unimplemented, "NFSv4 is not supported on this platform")
}
