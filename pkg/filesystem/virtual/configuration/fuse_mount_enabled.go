//go:build linux
// +build linux

package configuration

import (
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/fuse"
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *fuseMount) Expose(terminationGroup program.Group, rootDirectory virtual.Directory) error {
	// Parse configuration options.
	var directoryEntryValidity time.Duration
	if d := m.configuration.DirectoryEntryValidity; d != nil {
		if err := d.CheckValid(); err != nil {
			util.StatusWrap(err, "Failed to parse directory entry validity")
		}
		directoryEntryValidity = d.AsDuration()
	}
	var inodeAttributeValidity time.Duration
	if d := m.configuration.InodeAttributeValidity; d != nil {
		if err := d.CheckValid(); err != nil {
			util.StatusWrap(err, "Failed to parse inode attribute validity")
		}
		inodeAttributeValidity = d.AsDuration()
	}

	authenticator := fuse.AllowAuthenticator
	if expression := m.configuration.InHeaderAuthenticationMetadataJmespathExpression; expression != nil {
		compiledExpression, err := jmespath.NewExpressionFromConfiguration(expression, terminationGroup, clock.SystemClock)
		if err != nil {
			return util.StatusWrap(err, "Failed to compile in-header authentication metadata JMESPath expression")
		}
		authenticator = fuse.NewInHeaderAuthenticator(compiledExpression)
	}

	// Launch the FUSE server.
	removeStaleMounts(m.mountPath)
	deterministicTimestamp := uint64(filesystem.DeterministicFileModificationTimestamp.Unix())
	mountOptions := &go_fuse.MountOptions{
		// The name isn't strictly necessary, but is
		// filled in to prevent runc from crashing with
		// this error:
		// https://github.com/opencontainers/runc/blob/v1.0.0-rc10/libcontainer/mount/mount_linux.go#L69
		//
		// Newer versions of runc use an improved parser
		// that's more reliable:
		// https://github.com/moby/sys/blob/master/mountinfo/mountinfo_linux.go
		FsName:     m.fsName,
		AllowOther: m.configuration.AllowOther,
		// Speed up workloads that perform many tiny
		// writes. This means data is only guaranteed to
		// make it into the virtual file system after
		// calling close()/fsync()/munmap()/msync().
		EnableWritebackCache: true,
	}

	switch m.configuration.MountMethod {
	case pb.FUSEMountConfiguration_FUSERMOUNT:
		// go-fuse uses fusermount by default.
	case pb.FUSEMountConfiguration_DIRECT:
		mountOptions.DirectMountStrict = true
	case pb.FUSEMountConfiguration_DIRECT_AND_FUSERMOUNT:
		mountOptions.DirectMount = true
	default:
		return status.Error(codes.InvalidArgument, "Invalid mount method")
	}

	server, err := go_fuse.NewServer(
		fuse.NewMetricsRawFileSystem(
			fuse.NewDefaultAttributesInjectingRawFileSystem(
				fuse.NewSimpleRawFileSystem(
					rootDirectory,
					m.handleAllocator.RegisterRemovalNotifier,
					authenticator),
				directoryEntryValidity,
				inodeAttributeValidity,
				&go_fuse.Attr{
					Atime: deterministicTimestamp,
					Ctime: deterministicTimestamp,
					Mtime: deterministicTimestamp,
				}),
			clock.SystemClock),
		m.mountPath,
		mountOptions,
	)
	if err != nil {
		return util.StatusWrap(err, "Failed to create FUSE server")
	}
	// TODO: Run this as part of the program.Group, so that it gets
	// cleaned up upon shutdown.
	go server.Serve()

	// Adjust configuration options that can only be set after the
	// FUSE server has been launched.
	if err := fuse.SetLinuxBackingDevInfoTunables(
		m.mountPath,
		m.configuration.LinuxBackingDevInfoTunables,
	); err != nil {
		return util.StatusWrap(err, "Failed to set Linux Backing Device Info tunables")
	}
	return nil
}
