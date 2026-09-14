package runner

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func killPersistentWorkerProcess(process *os.Process) error {
	var killError error
	if err := process.WithHandle(func(handle uintptr) {
		processHandle := windows.Handle(handle)
		err := windows.TerminateProcess(processHandle, 1)
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			if state, waitError := windows.WaitForSingleObject(processHandle, 0); waitError == nil && state == windows.WAIT_OBJECT_0 {
				err = nil
			}
		}
		killError = os.NewSyscallError("TerminateProcess", err)
	}); err != nil {
		return os.ErrProcessDone
	}
	return killError
}
