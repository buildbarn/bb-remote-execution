//go:build darwin
// +build darwin

package configuration

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"
	"unsafe"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/util"
	nfs_sys_prot "github.com/buildbarn/go-xdr/pkg/protocols/darwin_nfs_sys_prot"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	initializeNFSOnce            sync.Once
	initializeNFSReadlinkNoCache sync.Once

	macOSBuildVersionPattern = regexp.MustCompile("^([0-9]+)([A-Z])([0-9]+)")
)

func toNfstime32(d time.Duration) *nfs_sys_prot.Nfstime32 {
	nanos := d.Nanoseconds()
	return &nfs_sys_prot.Nfstime32{
		Seconds:  int32(nanos / 1e9),
		Nseconds: uint32(nanos % 1e9),
	}
}

// getMacOSBuildVersion returns the build version of the currently
// running instance of macOS. For example, on macOS 13.0.1, it will
// return (22, 'A', 400).
func getMacOSBuildVersion() (int64, byte, int64, error) {
	osVersion, err := unix.Sysctl("kern.osversion")
	if err != nil {
		return 0, 0, 0, util.StatusWrap(err, "Failed to obtain the version of macOS running on the system")
	}
	submatches := macOSBuildVersionPattern.FindStringSubmatch(osVersion)
	if submatches == nil {
		return 0, 0, 0, status.Errorf(codes.Internal, "Cannot parse macOS version %#v", osVersion)
	}
	major, err := strconv.ParseInt(submatches[1], 10, 64)
	if err != nil {
		return 0, 0, 0, util.StatusWrapf(err, "Invalid macOS major version %#v", submatches[1])
	}
	daily, err := strconv.ParseInt(submatches[3], 10, 64)
	if err != nil {
		return 0, 0, 0, util.StatusWrapf(err, "Invalid macOS daily version %#v", submatches[3])
	}
	return major, submatches[2][0], daily, nil
}

func (m *nfsv4Mount) mount(terminationContext context.Context, terminationGroup *errgroup.Group, rpcServer *rpcserver.Server) error {
	// Extract the version of macOS used. We need to know this, as
	// it determines which mount options are supported.
	osMajor, osMinor, osDaily, err := getMacOSBuildVersion()
	if err != nil {
		return err
	}

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
	useUNIXSocket := osMajor > 22 || (osMajor == 22 && (osMinor > 'E' || (osMinor == 'E' && osDaily >= 118)))
	var sock net.Listener
	if useUNIXSocket {
		// Launch NFSv4 server on a UNIX socket.
		if err := os.Remove(osConfiguration.SocketPath); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapf(err, "Could not remove stale socket for NFSv4 server %#v", osConfiguration.SocketPath)
		}
		sock, err = net.Listen("unix", osConfiguration.SocketPath)
	} else {
		// Launch NFSv4 server on a TCP socket.
		// TODO: Remove this once UNIX socket support is stable.
		sock, err = net.Listen("tcp", "localhost:")
	}
	if err != nil {
		return util.StatusWrap(err, "Failed to create listening socket for NFSv4 server")
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
	attrMask := make(nfs_sys_prot.Bitmap, nfs_sys_prot.NFS_MATTR_BITMAP_LEN)
	var attrVals bytes.Buffer

	// Don't bother setting up a callback service, as we don't issue
	// CB_NOTIFY operations. Using this option is also a requirement
	// for making NFSv4 over UNIX sockets work.
	attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_FLAGS
	flags := nfs_sys_prot.NfsMattrFlags{
		Mask: []uint32{
			1 << nfs_sys_prot.NFS_MFLAG_NOCALLBACK,
		},
		Value: []uint32{
			1 << nfs_sys_prot.NFS_MFLAG_NOCALLBACK,
		},
	}
	flags.WriteTo(&attrVals)

	// Explicitly request the use of NFSv4.0.
	attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_NFS_VERSION
	nfs_sys_prot.WriteNfsMattrNfsVersion(&attrVals, 4)
	attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_NFS_MINOR_VERSION
	nfs_sys_prot.WriteNfsMattrNfsMinorVersion(&attrVals, 0)

	// The bb_virtual_tmp service exposes a symbolic link whose
	// contents should under no condition be cached by the kernel.
	// This requires us to both disable attribute caching for
	// regular files, and to set the readlink cache mode to fully
	// uncached (see below).
	if m.containsSelfMutatingSymlinks {
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_REG_MIN
		toNfstime32(0).WriteTo(&attrVals)
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_REG_MAX
		toNfstime32(0).WriteTo(&attrVals)
	}

	if d := osConfiguration.MinimumDirectoriesAttributeCacheTimeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid minimum directories attribute cache timeout")
		}
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_DIR_MIN
		toNfstime32(d.AsDuration()).WriteTo(&attrVals)
	}
	if d := osConfiguration.MaximumDirectoriesAttributeCacheTimeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid maximum directories attribute cache timeout")
		}
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_ATTRCACHE_DIR_MAX
		toNfstime32(d.AsDuration()).WriteTo(&attrVals)
	}

	if useUNIXSocket {
		// "ticotsord" is the X/Open Transport Interface (XTI)
		// equivalent of AF_LOCAL with SOCK_STREAM.
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_SOCKET_TYPE
		nfs_sys_prot.WriteNfsMattrSocketType(&attrVals, "ticotsord")
	}

	if !useUNIXSocket {
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_NFS_PORT
		nfs_sys_prot.WriteNfsMattrNfsPort(&attrVals, nfs_sys_prot.NfsMattrNfsPort(sock.Addr().(*net.TCPAddr).Port))
	}

	attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_FS_LOCATIONS
	var serverAddress string
	if useUNIXSocket {
		serverAddress = osConfiguration.SocketPath
	} else {
		serverAddress = sock.Addr().(*net.TCPAddr).IP.String()
	}
	fsLocations := nfs_sys_prot.NfsFsLocations{
		NfslLocation: []nfs_sys_prot.NfsFsLocation{{
			NfslServer: []nfs_sys_prot.NfsFsServer{{
				NfssAddress: []string{serverAddress},
			}},
		}},
	}
	fsLocations.WriteTo(&attrVals)

	if useUNIXSocket {
		attrMask[0] |= 1 << nfs_sys_prot.NFS_MATTR_LOCAL_NFS_PORT
		nfs_sys_prot.WriteNfsMattrLocalNfsPort(&attrVals, osConfiguration.SocketPath)
	}

	if m.containsSelfMutatingSymlinks {
		// Disable caching of symlinks. Depending on the version
		// of macOS, this can either be controlled by passing in
		// a mount option, or by changing a system-wide sysctl.
		if osMajor > 22 || (osMajor == 22 && (osMinor > 'E' || (osMinor == 'E' && osDaily >= 194))) {
			attrMask[1] |= 1 << (nfs_sys_prot.NFS_MATTR_READLINK_NOCACHE - 32)
			nfs_sys_prot.NFS_READLINK_CACHE_MODE_FULLY_UNCACHED.WriteTo(&attrVals)
		} else {
			initializeNFSReadlinkNoCache.Do(func() {
				exec.Command("/usr/sbin/sysctl", "vfs.generic.nfs.client.readlink_nocache=2").Run()
			})
		}
	}

	// Construct the nfs_mount_args message and serialize it.
	for attrMask[len(attrMask)-1] == 0 {
		attrMask = attrMask[:len(attrMask)-1]
	}
	mountArgs := nfs_sys_prot.NfsMountArgs{
		ArgsVersion:    nfs_sys_prot.NFS_ARGSVERSION_XDR,
		XdrArgsVersion: nfs_sys_prot.NFS_XDRARGS_VERSION_0,
		NfsMountAttrs: nfs_sys_prot.NfsMattr{
			Attrmask: attrMask,
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
