package runner

import (
	"os/exec"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

func TestPrepareCommandForStartAppendsJob(t *testing.T) {
	const existingJob syscall.Handle = 123
	jobs := make([]syscall.Handle, 1, 2)
	jobs[0] = existingJob
	cmd := exec.Command("does-not-need-to-exist")
	originalSysProcAttr := &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NO_WINDOW,
		Jobs:          jobs,
	}
	cmd.SysProcAttr = originalSysProcAttr

	commandProcess, err := prepareCommandForStart(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer commandProcess.Close()

	if cmd.SysProcAttr == originalSysProcAttr {
		t.Fatal("prepareCommandForStart() reused the caller's SysProcAttr")
	}
	if got, want := cmd.SysProcAttr.CreationFlags, uint32(windows.CREATE_NO_WINDOW|windows.CREATE_NEW_PROCESS_GROUP); got != want {
		t.Errorf("CreationFlags = %#x, want %#x", got, want)
	}
	if got, want := cmd.SysProcAttr.Jobs, []syscall.Handle{existingJob, syscall.Handle(commandProcess.job)}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Jobs = %v, want %v", got, want)
	}
	if got, want := originalSysProcAttr.Jobs, []syscall.Handle{existingJob}; len(got) != len(want) || got[0] != want[0] {
		t.Errorf("original Jobs = %v, want %v", got, want)
	}
	if got := jobs[:cap(jobs)][1]; got != 0 {
		t.Errorf("caller-owned Jobs backing array was modified: jobs[1] = %v", got)
	}
}
