//go:build darwin
// +build darwin

package configuration

import (
	"bytes"
	"context"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
	"unsafe"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/util"
	nfs_sys_prot "github.com/buildbarn/go-xdr/pkg/protocols/darwin_nfs_sys_prot"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"
	"github.com/buildbarn/go-xdr/pkg/runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	initializeNFSOnce            sync.Once
	initializeNFSReadlinkNoCache sync.Once
)

func toNfstime32(d time.Duration) *nfs_sys_prot.Nfstime32 {
	nanos := d.Nanoseconds()
	return &nfs_sys_prot.Nfstime32{
		Seconds:  int32(nanos / 1e9),
		Nseconds: uint32(nanos % 1e9),
	}
}

func (m *nfsv4Mount) mount(terminationContext context.Context, terminationGroup *errgroup.Group, rpcServer *rpcserver.Server) error {
	// macOS may require us to perform certain initialisation steps
	// before attempting to create the NFS mount, such as loading
	// the kernel extension containing the NFS client.
	//
	// Instead of trying to mimic those steps, call mount_nfs(8) in
	// such a way that the arguments are valid, but is guaranteed to
	// fail quickly.
	initializeNFSOnce.Do(func() {
		exec.Command("/sbin/mount_nfs", "0.0.0.0:/", "/").Run()
	})

	darwinConfiguration, ok := m.configuration.OperatingSystem.(*pb.NFSv4MountConfiguration_Darwin)
	if !ok {
		return status.Error(codes.InvalidArgument, "Darwin specific NFSv4 server configuration options not provided")
	}

	// Expose the NFSv4 server on the network.
	osConfiguration := darwinConfiguration.Darwin
	socketPath := osConfiguration.SocketPath
	var sock net.Listener
	var err error
	if socketPath == "" {
		// Launch NFSv4 server on a TCP socket.
		// TODO: Remove this once UNIX socket support is stable.
		sock, err = net.Listen("tcp", "localhost:")
	} else {
		// Launch NFSv4 server on a UNIX socket.
		if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapf(err, "Could not remove stale socket for NFSv4 server %#v", socketPath)
		}
		sock, err = net.Listen("unix", socketPath)
	}
	if err != nil {
		return util.StatusWrapf(err, "Failed to create listening socket for NFSv4 server %#v", socketPath)
	}
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

	// Construct attributes that are provided to mount(2). For NFS,
	// these attributes are stored in an XDR message. Similar to how
	// NFSv4's fattr4 works, the attributes need to be emitted in
	// increasing order by bitmask field.
	var attrMask uint32
	attrVals := bytes.NewBuffer(nil)

	// Don't bother setting up a callback service, as we don't issue
	// CB_NOTIFY operations. Using this option is also a requirement
	// for making NFSv4 over UNIX sockets work.
	attrMask |= 1 << nfs_sys_prot.NFS_MATTR_FLAGS
	flags := nfs_sys_prot.NfsMattrFlags{
		Mask: []uint32{
			1 << nfs_sys_prot.NFS_MFLAG_NOCALLBACK,
		},
		Value: []uint32{
			1 << nfs_sys_prot.NFS_MFLAG_NOCALLBACK,
		},
	}
	flags.WriteTo(attrVals)

	// Explicitly request the use of NFSv4.0.
	attrMask |= 1 << nfs_sys_prot.NFS_MATTR_NFS_VERSION
	nfs_sys_prot.WriteNfsMattrNfsVersion(attrVals, 4)
	attrMask |= 1 << nfs_sys_prot.NFS_MATTR_NFS_MINOR_VERSION
	nfs_sys_prot.WriteNfsMattrNfsMinorVersion(attrVals, 0)

	// The bb_virtual_tmp service exposes a symbolic link whose
	// contents should under no condition be cached by the kernel.
	if m.containsSelfMutatingSymlinks {
		// TODO: Is it possible to configure this feature
		// through a mount option?
		initializeNFSReadlinkNoCache.Do(func() {
			exec.Command("/usr/sbin/sysctl", "vfs.generic.nfs.client.readlink_nocache=2").Run()
		})
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_REG_MIN
		toNfstime32(0).WriteTo(attrVals)
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_REG_MAX
		toNfstime32(0).WriteTo(attrVals)
	}

	if d := osConfiguration.MinimumDirectoriesAttributeCacheTimeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid minimum directories attribute cache timeout")
		}
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_DIR_MIN
		toNfstime32(d.AsDuration()).WriteTo(attrVals)
	}
	if d := osConfiguration.MaximumDirectoriesAttributeCacheTimeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid maximum directories attribute cache timeout")
		}
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_DIR_MAX
		toNfstime32(d.AsDuration()).WriteTo(attrVals)
	}

	if socketPath != "" {
		// "ticotsord" is the X/Open Transport Interface (XTI)
		// equivalent of AF_LOCAL with SOCK_STREAM.
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_SOCKET_TYPE
		nfs_sys_prot.WriteNfsMattrSocketType(attrVals, "ticotsord")
	}

	if socketPath == "" {
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_NFS_PORT
		nfs_sys_prot.WriteNfsMattrNfsPort(attrVals, nfs_sys_prot.NfsMattrNfsPort(sock.Addr().(*net.TCPAddr).Port))
	}

	attrMask |= 1 << nfs_sys_prot.NFS_MATTR_FS_LOCATIONS
	serverAddress := socketPath
	if socketPath == "" {
		serverAddress = sock.Addr().(*net.TCPAddr).IP.String()
	}
	fsLocations := nfs_sys_prot.NfsFsLocations{
		NfslLocation: []nfs_sys_prot.NfsFsLocation{{
			NfslServer: []nfs_sys_prot.NfsFsServer{{
				NfssAddress: []string{serverAddress},
			}},
		}},
	}
	fsLocations.WriteTo(attrVals)

	if socketPath != "" {
		attrMask |= 1 << nfs_sys_prot.NFS_MATTR_LOCAL_NFS_PORT
		runtime.WriteUTF8String(attrVals, math.MaxUint32, socketPath)
	}

	// Construct the nfs_mount_args message and serialize it.
	mountArgs := nfs_sys_prot.NfsMountArgs{
		ArgsVersion:    88, // NFS_ARGSVERSION_XDR.
		XdrArgsVersion: nfs_sys_prot.NFS_XDRARGS_VERSION_0,
		NfsMountAttrs: nfs_sys_prot.NfsMattr{
			Attrmask: nfs_sys_prot.Bitmap{attrMask},
			AttrVals: attrVals.Bytes(),
		},
	}
	mountArgs.ArgsLength = uint32(mountArgs.GetEncodedSizeBytes())

	mountArgsBuf := bytes.NewBuffer(make([]byte, 0, mountArgs.ArgsLength))
	if _, err := mountArgs.WriteTo(mountArgsBuf); err != nil {
		return util.StatusWrap(err, "Failed to marshal NFS mount arguments")
	}

	// Call mount(2) with the serialized nfs_mount_args message.
	mountPath := m.mountPath
	unix.Unmount(mountPath, 0)
	if err := unix.Mount("nfs", mountPath, 0, unsafe.Pointer(&mountArgsBuf.Bytes()[0])); err != nil {
		return util.StatusWrap(err, "Mounting NFS volume failed")
	}

	// Automatically unmount upon shutdown.
	terminationGroup.Go(func() error {
		<-terminationContext.Done()
		if err := unix.Unmount(mountPath, 0); err != nil {
			return util.StatusWrapf(err, "Failed to unmount %#v", mountPath)
		}
		return nil
	})
	return nil
}
