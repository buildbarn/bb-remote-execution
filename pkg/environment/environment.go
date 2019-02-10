package environment

import (
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// Environment represents a context in which build commands may be
// invoked. Examples of environments may include Docker containers,
// simple chroots, local execution, etc., etc.
type Environment interface {
	// Run a command within the environment, with a given set of
	// arguments, environment variables, a working directory
	// relative to the build directory and a pair of pathnames for
	// writing stdout/stderr diagnostics output.
	runner.RunnerServer

	// GetBuildDirectory returns a handle to a directory in which a
	// BuildExecutor may place the input files of the build step and
	// where output files created by the build action are stored.
	GetBuildDirectory() filesystem.Directory
}
