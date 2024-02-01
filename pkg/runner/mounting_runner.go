package runner

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mountingRunner struct {
	base           runner_pb.RunnerServer
	buildDirectory filesystem.Directory
	mount          *bb_runner.InputMountOptions
}

// NewMountingRunner is a decorator for Runner
// that mounts `mount` before running a build action.
//
// This decorator can be used for chroot runners
// that must mount special filesystems into the input root.
func NewMountingRunner(base runner_pb.RunnerServer, buildDirectory filesystem.Directory, mount *bb_runner.InputMountOptions) runner_pb.RunnerServer {
	return &mountingRunner{
		buildDirectory: buildDirectory,
		mount:          mount,
		base:           base,
	}
}

func (r *mountingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (response *runner_pb.RunResponse, err error) {
	rootResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer rootResolver.closeAll()
	inputRootDirectory, err := path.NewUNIXParser(request.InputRootDirectory)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid input root directory path")
	}
	if err := path.Resolve(inputRootDirectory, path.NewRelativeScopeWalker(&rootResolver)); err != nil {
		return nil, util.StatusWrap(err, "Invalid input root.")
	}

	inputRoot := rootResolver.stack.Peek()
	terminal := rootResolver.TerminalName
	if terminal != nil {
		inputRoot, err = inputRoot.EnterDirectory(*terminal)
		if err != nil {
			return nil, util.StatusWrap(err, "Invalid input root.")
		}
	}

	defer inputRoot.Close()

	mountResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(inputRoot)),
	}
	defer mountResolver.closeAll()
	mountpoint, err := path.NewUNIXParser(r.mount.Mountpoint)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid mountpoint path.")
	}
	if err := path.Resolve(mountpoint, path.NewRelativeScopeWalker(&mountResolver)); err != nil {
		return nil, util.StatusWrap(err, "Invalid mountpoint directory path.")
	}

	mountDir := mountResolver.stack.Peek()
	defer mountDir.Close()
	if mountResolver.TerminalName == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Could not resolve mountpoint basename: %#v", r.mount.Mountpoint)
	}

	mountname := *mountResolver.TerminalName
	if err := mountDir.Mount(mountname, r.mount.Source, r.mount.FilesystemType); err != nil {
		return nil, util.StatusWrapf(err, "Failed to mount %#v in the input root", r.mount.Mountpoint)
	}

	// We only need the mounting directory file descriptor open.
	rootResolver.closeAll()
	// Already peeked the mounting directory
	// we must now pop it to save it from the close iteration.
	maybe, ok := mountResolver.stack.PopSingle()
	if ok {
		// PopSingle does not return anything if it is the initial element in the stack,
		// which we got from Peek earlier.
		defer maybe.Close()
		mountDir = maybe
	}
	mountResolver.closeAll()

	response, err = r.base.Run(ctx, request)
	if err != nil {
		return nil, err
	}
	if err2 := mountDir.Unmount(mountname); err2 != nil {
		err = util.StatusFromMultiple([]error{
			err,
			util.StatusWrapf(err2, "Failed to unmount %#v in the input root", r.mount.Mountpoint),
		})
	}

	return response, nil
}

func (r *mountingRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (*emptypb.Empty, error) {
	return r.base.CheckReadiness(ctx, request)
}
