package environment

import (
	"github.com/buildbarn/bb-storage/pkg/util"
)

// Manager is a factory for Environments in which build actions are run. An
// Manager has access to platform properties passed to the command to be
// executed. This may allow the Manager to, for example, download container
// images or set up simulators/emulators.
type Manager interface {
	Acquire(actionDigest *util.Digest, platform map[string]string) (ManagedEnvironment, error)
}

// ManagedEnvironment is an environment that is owned by a Manager.
// After use, it must be released, so that resources associated with it
// (e.g., a running container, a build directory) may be destroyed.
type ManagedEnvironment interface {
	Environment

	// Release the Environment back to the EnvironmentManager,
	// causing any input/output files to be discarded.
	Release()
}
