package runner

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandProcessCancellationBetweenCheckAndResume(t *testing.T) {
	// Pause AfterStart after it has checked for cancellation, at which point it
	// must still hold the mutex. Cancel must block until AfterStart resumes the
	// process and releases the mutex. The old implementation released the mutex
	// before resume, allowing Cancel to terminate the suspended process first.
	cmd := exec.Command(
		filepath.Join(os.Getenv("SYSTEMROOT"), "System32", "cmd.exe"),
		"/d", "/c", "ping -n 60 127.0.0.1 > nul",
	)
	commandProcess, err := prepareCommandForStart(cmd)
	if err != nil {
		t.Fatal(err)
	}

	afterCancellationCheck := make(chan struct{})
	allowResume := make(chan struct{})
	commandProcess.afterCancellationCheck = func() {
		close(afterCancellationCheck)
		<-allowResume
	}

	if err := cmd.Start(); err != nil {
		commandProcess.Close()
		t.Fatal(err)
	}
	afterStartResult := make(chan error, 1)
	go func() {
		afterStartResult <- commandProcess.AfterStart(cmd)
	}()

	select {
	case <-afterCancellationCheck:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the cancellation check")
	}

	var cancelErr error
	cancellationReturnedBeforeResume := commandProcess.lock.TryLock()
	var cancelResult chan error
	if cancellationReturnedBeforeResume {
		commandProcess.lock.Unlock()
		cancelErr = commandProcess.Cancel()
	} else {
		cancelResult = make(chan error, 1)
		go func() {
			cancelResult <- commandProcess.Cancel()
		}()
	}
	close(allowResume)

	var afterStartErr error
	select {
	case afterStartErr = <-afterStartResult:
	case <-time.After(10 * time.Second):
		t.Fatal("AfterStart() hung")
	}
	if !cancellationReturnedBeforeResume {
		select {
		case cancelErr = <-cancelResult:
		case <-time.After(10 * time.Second):
			t.Fatal("Cancel() hung")
		}
	}

	if cancelErr != nil && !errors.Is(cancelErr, os.ErrProcessDone) {
		t.Errorf("Cancel() failed: %v", cancelErr)
	}
	if afterStartErr != nil {
		t.Errorf("AfterStart() failed: %v", afterStartErr)
	}
	if cancellationReturnedBeforeResume {
		t.Error("cancellation terminated the suspended job before resume")
	}

	if afterStartErr == nil {
		_ = cmd.Wait()
		if err := commandProcess.AfterWait(cmd); err != nil {
			t.Errorf("AfterWait() failed: %v", err)
		}
	}
}
