package util_test

import (
	"context"
	"errors"
	"testing"

	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestSharedActionQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("RunOneItemSuccess", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(10)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 1
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(nil)
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.NoError(t, result)
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})

	t.Run("TwoRunsFirstOneFailsSecondSucceeds", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(10)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 2
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(errors.New("Error42"))
		})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(nil)
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.EqualError(t, result, "Error42")
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})

	t.Run("TwoRunsFirstOneSucceedsSecondFails", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(10)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 2
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(nil)
		})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(errors.New("Error42-1"))
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.EqualError(t, result, "Error42-1")
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})

	// Note this test we invert the order so the first Action resolves after
	// the second.
	t.Run("TwoRunsBothFailOnlyFirstFailureReturned", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(10)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 2
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		waitForSecondThreadToFinishChan := make(chan struct{})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			<-waitForSecondThreadToFinishChan
			didRunActionChan <- struct{}{}
			ac.Done(errors.New("Error42-1"))
		})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			ac.Done(errors.New("Error42-2"))
			waitForSecondThreadToFinishChan <- struct{}{}
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.EqualError(t, result, "Error42-2")
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})

	t.Run("ProcessActionQueueWhileExecuting", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(10)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 3
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			actionGroup2 := re_util.ActionGroup{}
			doneHandler := ac.Defer()
			go func() {
				actionGroup2 = append(actionGroup2, func(ac *re_util.ActionContext) {
					didRunActionChan <- struct{}{}
					ac.Done(nil)
				})
				actionGroup2 = append(actionGroup2, func(ac *re_util.ActionContext) {
					didRunActionChan <- struct{}{}
					ac.Done(nil)
				})
				err := saq.ProcessActionQueue(ac.Ctx(), actionGroup2).WaitForResult()
				doneHandler(err)
			}()
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.NoError(t, result)
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})

	t.Run("EnsureActionGroupContextCancelPropagatesToOtherContexts", func(t *testing.T) {
		saq := re_util.NewSharedActionQueue(2)
		actionGroup := re_util.ActionGroup{}
		expectedActionsExecuted := 3
		didRunActionChan := make(chan struct{}, expectedActionsExecuted)
		innerActionStartedChan := make(chan struct{})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			actionGroup2 := re_util.ActionGroup{}
			doneHandler := ac.Defer()
			go func() {
				actionGroup2 = append(actionGroup2, func(ac *re_util.ActionContext) {
					innerActionStartedChan <- struct{}{}
					didRunActionChan <- struct{}{}
					ac.Done(errors.New("SomeError2"))
				})
				err := saq.ProcessActionQueue(ac.Ctx(), actionGroup2).WaitForResult()
				doneHandler(err)
			}()
		})
		actionGroup = append(actionGroup, func(ac *re_util.ActionContext) {
			didRunActionChan <- struct{}{}
			<-innerActionStartedChan // Wait for inner action to start.
			<-ac.Ctx().Done()        // Wait for context to be canceled.
			ac.Done(errors.New("WeGotCancelledOtherErrorShouldBeTheVisibleError"))
		})
		result := saq.ProcessActionQueue(context.Background(), actionGroup).WaitForResult()
		require.EqualError(t, result, "SomeError2")
		require.Equal(t, len(didRunActionChan), expectedActionsExecuted)
	})
}
