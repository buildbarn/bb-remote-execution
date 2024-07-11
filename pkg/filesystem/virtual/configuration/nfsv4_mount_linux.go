//go:build linux
// +build linux

package configuration

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *nfsv4Mount) mount(terminationGroup program.Group, rpcServer *rpcserver.Server) error {
	linuxConfiguration, ok := m.configuration.OperatingSystem.(*pb.NFSv4MountConfiguration_Linux)
	if !ok {
		return status.Error(codes.InvalidArgument, "Linux specific NFSv4 server configuration options not provided")
	}
	osConfiguration := linuxConfiguration.Linux

	// Expose the NFSv4 server on a TCP socket.
	sock, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return util.StatusWrap(err, "Failed to create listening socket for NFSv4 server")
	}
	// TODO: Run this as part of the program.Group, so that it gets
	// cleaned up upon shutdown.
	go func() {
		for {
			c, err := sock.Accept()
			if err != nil {
				log.Print("Got accept error: ", err)
			}
			go func() {
				err := rpcServer.HandleConnection(c, c)
				c.Close()
				if err != nil {
					log.Print("Failure handling NFSv4 connection: ", err)
				}
			}()
		}
	}()

	// Mount the volume exported by the NFSv4 server.
	mountPath := m.mountPath
	removeStaleMounts(mountPath)
	listenAddressPort := sock.Addr().(*net.TCPAddr).AddrPort()
	if err := unix.Mount(
		/* source = */ ":/", // fmt.Sprintf("[%s]:/", listenAddressPort.Addr()),
		/* target = */ mountPath,
		/* fsType = */ "nfs4",
		/* flags = */ 0,
		/* data = */ strings.Join(
			append(
				[]string{
					"addr=" + listenAddressPort.Addr().String(),
					fmt.Sprintf("port=%d", listenAddressPort.Port()),
				},
				osConfiguration.MountOptions...,
			),
			","),
	); err != nil {
		return util.StatusWrap(err, "Mounting NFS volume failed")
	}

	// Automatically unmount upon shutdown.
	terminationGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		<-ctx.Done()
		if err := unix.Unmount(mountPath, 0); err != nil {
			return util.StatusWrapf(err, "Failed to unmount %#v", mountPath)
		}
		return nil
	})
	return nil
}
