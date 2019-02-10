package environment

import (
	"github.com/buildbarn/bb-storage/pkg/util"
)

type singletonManager struct {
	environment chan ManagedEnvironment
}

// NewSingletonManager is a simple Manager that always returns the same
// Environment. This is typically used in combination with
// NewLocalExecutionEnvironment or NewRemoteExecutionManager to force
// that all build actions are executed using the same method.
func NewSingletonManager(environment Environment) Manager {
	em := &singletonManager{
		environment: make(chan ManagedEnvironment, 1),
	}
	em.environment <- &singletonEnvironment{
		Environment: environment,
		manager:     em,
	}
	return em
}

func (em *singletonManager) Acquire(actionDigest *util.Digest, platformProperties map[string]string) (ManagedEnvironment, error) {
	return <-em.environment, nil
}

type singletonEnvironment struct {
	Environment
	manager *singletonManager
}

func (e *singletonEnvironment) Release() {
	// Never call Release() on the underlying environment, as it
	// will be reused.
	e.manager.environment <- e
}
