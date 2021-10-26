package runner

import (
	"context"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// logFileResolver is an implementation of path.ComponentWalker that is
// used by localRunner.Run() to traverse to the directory of stdout and
// stderr log files, so that they may be opened.
//
// TODO: This code seems fairly generic. Should move it to the
// filesystem package?
type logFileResolver struct {
	stack []filesystem.DirectoryCloser
	name  *path.Component
}

func (r *logFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.stack[len(r.stack)-1]
	child, err := d.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	r.stack = append(r.stack, child)
	return path.GotDirectory{
		Child:        r,
		IsReversible: true,
	}, nil
}

func (r *logFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	r.name = &name
	return nil, nil
}

func (r *logFileResolver) OnUp() (path.ComponentWalker, error) {
	if len(r.stack) == 1 {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the build directory")
	}
	if err := r.stack[len(r.stack)-1].Close(); err != nil {
		return nil, err
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r, nil
}

func (r *logFileResolver) closeAll() {
	for _, d := range r.stack {
		d.Close()
	}
}

type localRunner struct {
	buildDirectory               filesystem.Directory
	buildDirectoryPath           *path.Builder
	sysProcAttr                  *syscall.SysProcAttr
	setTmpdirEnvironmentVariable bool
	chrootIntoInputRoot          bool
}

// NewLocalRunner returns a Runner capable of running commands on the
// local system directly.
func NewLocalRunner(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, sysProcAttr *syscall.SysProcAttr, setTmpdirEnvironmentVariable, chrootIntoInputRoot bool) runner.RunnerServer {
	return &localRunner{
		buildDirectory:               buildDirectory,
		buildDirectoryPath:           buildDirectoryPath,
		sysProcAttr:                  sysProcAttr,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
		chrootIntoInputRoot:          chrootIntoInputRoot,
	}
}

func (r *localRunner) openLog(logPath string) (filesystem.FileAppender, error) {
	logFileResolver := logFileResolver{
		stack: []filesystem.DirectoryCloser{filesystem.NopDirectoryCloser(r.buildDirectory)},
	}
	defer logFileResolver.closeAll()
	if err := path.Resolve(logPath, path.NewRelativeScopeWalker(&logFileResolver)); err != nil {
		return nil, err
	}
	if logFileResolver.name == nil {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}
	d := logFileResolver.stack[len(logFileResolver.stack)-1]
	return d.OpenAppend(*logFileResolver.name, filesystem.CreateExcl(0o666))
}

func (r *localRunner) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	return r.run(ctx, request)
}

func (r *localRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
