package runner

import (
	"context"
	"os/exec"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// NewRemappingCommandCreator returns a CommandCreator for cases where we need
// to remap the executable path. This is often the case with multi-platform builds
// where Bazel does not set different shell programs for the different platforms.
// Its `--default_shell_executable` is applied for _all_ actions.
// TODO: This should be fixed in Bazel to avoid this workaround.
func NewRemappingCommandCreator(remap map[string]string, inner CommandCreator) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryStr, pathVariable string) (*exec.Cmd, error) {
		argv0 := arguments[0]
		if new, ok := remap[argv0]; ok {
			argv0 = new
		}
		arguments[0] = argv0

		return inner(ctx, arguments, inputRootDirectory, workingDirectoryStr, pathVariable)
	}
}
