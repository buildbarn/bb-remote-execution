//go:build freebsd || windows
// +build freebsd windows

package fuse

import (
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/fuse"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stubs for APIs that aren't available on platforms that don't support
// FUSE. We should see if there's anything we can do to make this list
// smaller. Can we weaken the coupling between cmd/bb_worker/main.go and
// pkg/filesystem/fuse?

type Directory interface{}

type EntryNotifier func()

type FileAllocator interface{}

type InitialContentsFetcher interface{}

type Leaf interface{}

type PrepopulatedDirectory interface {
	RemoveAllChildren(forbidNewChildren bool) error
}

type SimpleRawFileSystemServerCallbacks struct{}

func (sc *SimpleRawFileSystemServerCallbacks) EntryNotify() {
	panic("FUSE is not supported on this platform")
}

func NewInMemoryPrepopulatedDirectory(fileAllocator FileAllocator, errorLogger util.ErrorLogger, rootInodeNumber uint64, inodeNumberGenerator random.ThreadSafeGenerator, entryNotifier EntryNotifier) PrepopulatedDirectory {
	return nil
}

func NewMountFromConfiguration(configuration *pb.MountConfiguration, rootDirectory Directory, rootDirectoryInodeNumber uint64, serverCallbacks *SimpleRawFileSystemServerCallbacks, fsName string) error {
	return status.Error(codes.Unimplemented, "FUSE is not supported on this platform")
}

func NewPoolBackedFileAllocator(pool re_filesystem.FilePool, errorLogger util.ErrorLogger, inodeNumberGenerator random.ThreadSafeGenerator) FileAllocator {
	return nil
}
