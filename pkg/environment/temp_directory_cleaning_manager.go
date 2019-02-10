package environment

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type tempDirectoryCleaningManager struct {
	base          Manager
	tempDirectory filesystem.Directory
}

// NewTempDirectoryCleaningManager is an adapter for Manager that upon
// acquisition empties out a temporary directory (e.g. /tmp, /var/tmp, etc).
// This ensures that the build action is executed in a clean environment.
func NewTempDirectoryCleaningManager(base Manager, tempDirectory filesystem.Directory) Manager {
	return &tempDirectoryCleaningManager{
		base:          base,
		tempDirectory: tempDirectory,
	}
}

func (em *tempDirectoryCleaningManager) Acquire(actionDigest *util.Digest, platformProperties map[string]string) (ManagedEnvironment, error) {
	// Remove all contents prior to use.
	if err := em.tempDirectory.RemoveAllChildren(); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to clean temporary directory prior to build")
	}

	// Allocate underlying environment.
	return em.base.Acquire(actionDigest, platformProperties)
}
