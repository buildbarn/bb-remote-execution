// +build darwin linux

package fuse

import (
	"sort"
	"time"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/fuse"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewMountFromConfiguration creates a new FUSE mount based on options
// specified in a configuration message and starts processing of
// incoming requests.
func NewMountFromConfiguration(configuration *pb.MountConfiguration, rootDirectory Directory, rootDirectoryInodeNumber uint64, serverCallbacks *SimpleRawFileSystemServerCallbacks, fsName string) error {
	// Parse configuration options.
	var directoryEntryValidity time.Duration
	if d := configuration.DirectoryEntryValidity; d != nil {
		if err := d.CheckValid(); err != nil {
			util.StatusWrap(err, "Failed to parse directory entry validity")
		}
		directoryEntryValidity = d.AsDuration()
	}
	var inodeAttributeValidity time.Duration
	if d := configuration.DirectoryEntryValidity; d != nil {
		if err := d.CheckValid(); err != nil {
			util.StatusWrap(err, "Failed to parse inode attribute validity")
		}
		inodeAttributeValidity = d.AsDuration()
	}
	directorySorter := sort.Sort
	if configuration.ShuffleDirectoryListings {
		directorySorter = Shuffle
	}

	// Launch the FUSE server.
	deterministicTimestamp := uint64(filesystem.DeterministicFileModificationTimestamp.Unix())
	server, err := fuse.NewServer(
		NewMetricsRawFileSystem(
			NewDefaultAttributesInjectingRawFileSystem(
				NewSimpleRawFileSystem(
					rootDirectory,
					rootDirectoryInodeNumber,
					directorySorter,
					serverCallbacks),
				directoryEntryValidity,
				inodeAttributeValidity,
				&fuse.Attr{
					Atime: deterministicTimestamp,
					Ctime: deterministicTimestamp,
					Mtime: deterministicTimestamp,
				}),
			clock.SystemClock),
		configuration.MountPath,
		&fuse.MountOptions{
			// The name isn't strictly necessary, but is
			// filled in to prevent runc from crashing with
			// this error:
			// https://github.com/opencontainers/runc/blob/v1.0.0-rc10/libcontainer/mount/mount_linux.go#L69
			//
			// Newer versions of runc use an improved parser
			// that's more reliable:
			// https://github.com/moby/sys/blob/master/mountinfo/mountinfo_linux.go
			FsName:      fsName,
			AllowOther:  configuration.AllowOther,
			DirectMount: configuration.DirectMount,
		})
	if err != nil {
		return util.StatusWrap(err, "Failed to create FUSE server")
	}
	go server.Serve()

	// Adjust configuration options that can only be set after the
	// FUSE server has been launched.
	if configuration.MaximumDirtyPagesPercentage != 0 {
		if err := SetMaximumDirtyPagesPercentage(
			configuration.MountPath,
			int(configuration.MaximumDirtyPagesPercentage),
		); err != nil {
			return util.StatusWrap(err, "Failed to set maximum dirty pages percentage")
		}
	}
	return nil
}
