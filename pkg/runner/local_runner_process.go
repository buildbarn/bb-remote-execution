//go:build !windows
// +build !windows

package runner

import "os/exec"

type commandProcess struct{}

// prepareCommandForStart is called after the command is fully configured and
// before Start. Non-Windows platforms do not need extra process-tree state.
func prepareCommandForStart(cmd *exec.Cmd) (*commandProcess, error) {
	return &commandProcess{}, nil
}

// AfterStart is called after Start succeeds and before the caller waits.
func (commandProcess) AfterStart(cmd *exec.Cmd) error {
	return nil
}

// AfterWait is called after Wait returns, before build directory cleanup can
// proceed.
func (commandProcess) AfterWait(cmd *exec.Cmd) error {
	return nil
}

// Close releases any resources allocated by prepareCommandForStart. It must be
// safe to call if Start fails.
func (commandProcess) Close() {}
