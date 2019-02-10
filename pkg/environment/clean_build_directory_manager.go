package environment

import (
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type cleanBuildDirectoryManager struct {
	base Manager
}

// NewCleanBuildDirectoryManager is an adapter for Manager that upon
// acquistion empties out the build directory. This ensures that the
// build action is executed in a clean environment.
func NewCleanBuildDirectoryManager(base Manager) Manager {
	return &cleanBuildDirectoryManager{
		base: base,
	}
}

func (em *cleanBuildDirectoryManager) Acquire(actionDigest *util.Digest, platformProperties map[string]string) (ManagedEnvironment, error) {
	// Allocate underlying environment.
	environment, err := em.base.Acquire(actionDigest, platformProperties)
	if err != nil {
		return nil, err
	}

	// Remove all contents prior to use.
	if err := environment.GetBuildDirectory().RemoveAllChildren(); err != nil {
		environment.Release()
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to clean build directory prior to build")
	}
	return environment, nil
}
