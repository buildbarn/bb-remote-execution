package util

import (
	"context"
	"sync"
)

type ActionContext struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	results    chan error
	isWaiting  bool
}

type Action = func(*ActionContext)
type ActionGroup = []Action

type SharedActionQueue struct {
	sharedActionQueue chan func()
}

// This is a utility designed to run 'Action's to do some work async but limit
// by a pre-defined number. This allows multiple components to share the same
// pool of goroutines or other similar resources (like data streams) easily.
func NewSharedActionQueue(maxConcurrentDownloads uint32) *SharedActionQueue {
	sharedActionQueue := make(chan func(), maxConcurrentDownloads)
	for i := uint32(0); i < maxConcurrentDownloads; i++ {
		go func() {
			for {
				action, ok := <-sharedActionQueue
				if !ok {
					return // Looks like our channel is closed.
				}
				action()
			}
		}()
	}
	return &SharedActionQueue{
		sharedActionQueue: sharedActionQueue,
	}
}

// Enqueues all provided actions onto a shared limit action queue.
// Will only return the first error discovered and will always block
// until all actions are finished before returning.
func (saq *SharedActionQueue) ProcessActionQueue(ctx context.Context, actionGroup ActionGroup) *ActionContext {
	newCtx, cancelFunc := context.WithCancel(ctx)
	actionContext := ActionContext{
		ctx:        newCtx,
		cancelFunc: cancelFunc,
		wg:         sync.WaitGroup{},
		results:    make(chan error, len(actionGroup)),
		isWaiting:  false,
	}
	for _, action := range actionGroup {
		actionContext.wg.Add(1)
		actionCopy := action // Warning: Golang likes to capture by reference causing our lambda to get the same function.
		saq.sharedActionQueue <- func() {
			actionCopy(&actionContext)
		}
	}
	return &actionContext
}

func (ac *ActionContext) Ctx() context.Context {
	return ac.ctx
}

func (ac *ActionContext) Done(result error) {
	ac.results <- result
	if result != nil {
		ac.cancelFunc() // Trigger all actions in our ActionGroup to cancel.
	}
	ac.wg.Done()
}

func (ac *ActionContext) Defer() func(error) {
	return func(result error) {
		ac.Done(result)
	}
}

func (ac *ActionContext) WaitForResult() error {
	if ac.isWaiting { // Technically this isn't thread-safe, but its an assertion anyway.
		panic("Assertion failed. WaitForResult() can only be called once per ActionContext")
	}
	ac.isWaiting = true
	// Wait for all passed in actions to finish. We don't return on
	// first error in fear of introducing bugs around lifetime.
	ac.wg.Wait()
	close(ac.results)

	for err := range ac.results {
		if err != nil {
			return err
		}
	}
	return nil
}
