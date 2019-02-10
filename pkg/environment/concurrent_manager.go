package environment

import (
	"log"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
)

type concurrentManager struct {
	base        Manager
	lock        sync.Mutex
	refcount    uint
	environment ManagedEnvironment
}

// NewConcurrentManager is an adapter for Manager that causes concurrent
// acquisition of environments to use the same underlying Environment.
// By using reference counting, the underlying Environment is released
// only when the last consumer releases.
//
// This adapter is commonly used in combination with other adapters like
// ActionDigestSubdirectoryManager to ensure that a single build
// directory can be shared by concurrent build actions.
func NewConcurrentManager(base Manager) Manager {
	return &concurrentManager{
		base: base,
	}
}

func (em *concurrentManager) Acquire(actionDigest *util.Digest, platformProperties map[string]string) (ManagedEnvironment, error) {
	em.lock.Lock()
	defer em.lock.Unlock()
	if em.refcount == 0 {
		// No environment acquired yet. Call into the underlying manager.
		environment, err := em.base.Acquire(actionDigest, platformProperties)
		if err != nil {
			return nil, err
		}
		em.environment = &concurrentEnvironment{
			ManagedEnvironment: environment,
			manager:            em,
		}
	}
	em.refcount++
	return em.environment, nil
}

type concurrentEnvironment struct {
	ManagedEnvironment
	manager *concurrentManager
}

func (e *concurrentEnvironment) Release() {
	e.manager.lock.Lock()
	defer e.manager.lock.Unlock()
	if e.manager.refcount == 0 {
		log.Fatal("Attempted to release an already released environment")
	}
	e.manager.refcount--
	if e.manager.refcount == 0 {
		// Last consumer released. Release the underlying environment.
		e.ManagedEnvironment.Release()
	}
}
