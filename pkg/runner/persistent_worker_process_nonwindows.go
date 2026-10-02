//go:build !windows
// +build !windows

package runner

import "os"

func killPersistentWorkerProcess(process *os.Process) error {
	return process.Kill()
}
