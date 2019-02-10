package environment

import (
	"context"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localExecutionEnvironment struct {
	buildDirectory filesystem.Directory
	buildPath      string
}

// NewLocalExecutionEnvironment returns an Environment capable of running
// commands on the local system directly.
func NewLocalExecutionEnvironment(buildDirectory filesystem.Directory, buildPath string) Environment {
	return &localExecutionEnvironment{
		buildDirectory: buildDirectory,
		buildPath:      buildPath,
	}
}

func (e *localExecutionEnvironment) GetBuildDirectory() filesystem.Directory {
	return e.buildDirectory
}

func (e *localExecutionEnvironment) openLog(logPath string) (filesystem.File, error) {
	components := strings.FieldsFunc(logPath, func(r rune) bool { return r == '/' })
	if len(components) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient pathname components in filename")
	}

	// Traverse to directory where log should be created.
	d := e.buildDirectory
	for n, component := range components[:len(components)-1] {
		d2, err := d.Enter(component)
		if d != e.buildDirectory {
			d.Close()
		}
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to enter directory %#v", path.Join(components[:n+1]...))
		}
		d = d2
	}

	// Create log file within.
	f, err := d.OpenFile(components[len(components)-1], os.O_APPEND|os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if d != e.buildDirectory {
		d.Close()
	}
	return f, err
}

func (e *localExecutionEnvironment) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}
	cmd := exec.CommandContext(ctx, request.Arguments[0], request.Arguments[1:]...)
	// TODO(edsch): Convert workingDirectory to use platform
	// specific path delimiter.
	cmd.Dir = filepath.Join(e.buildPath, request.WorkingDirectory)
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Open output files for logging.
	stdout, err := e.openLog(request.StdoutPath)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to open stdout")
	}
	cmd.Stdout = stdout

	stderr, err := e.openLog(request.StderrPath)
	if err != nil {
		stdout.Close()
		return nil, util.StatusWrap(err, "Failed to open stderr")
	}
	cmd.Stderr = stderr

	// Start the subprocess. We can already close the output files
	// while the process is running.
	err = cmd.Start()
	stdout.Close()
	stderr.Close()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to start process")
	}

	// Wait for execution to complete.
	err = cmd.Wait()
	if exitError, ok := err.(*exec.ExitError); ok {
		waitStatus := exitError.Sys().(syscall.WaitStatus)
		return &runner.RunResponse{
			ExitCode: int32(waitStatus.ExitStatus()),
		}, nil
	}
	return &runner.RunResponse{
		ExitCode: 0,
	}, err
}
