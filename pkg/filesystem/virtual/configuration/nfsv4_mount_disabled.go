//go:build freebsd || linux || windows
// +build freebsd linux windows

package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getLatestSupportedNFSv4MinorVersion() (uint32, error) {
	return 0, nil
}

func (m *nfsv4Mount) mount(terminationGroup program.Group, rpcServer *rpcserver.Server, minorVersion uint32) error {
	return status.Error(codes.Unimplemented, "NFSv4 is not supported on this platform")
}
