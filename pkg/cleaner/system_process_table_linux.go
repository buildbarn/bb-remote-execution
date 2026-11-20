//go:build linux
// +build linux

package cleaner

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// atClkTck is the key of the auxiliary vector entry that holds the
// number of clock ticks per second (AT_CLKTCK in <linux/auxvec.h>).
// It is declared here, as golang.org/x/sys/unix does not provide
// constants for the keys of the auxiliary vector.
const atClkTck = 17

// getClockTicksPerSecond returns the unit in which the kernel reports
// process start times. This is what sysconf(_SC_CLK_TCK) returns. Like
// the C library, obtain it from the auxiliary vector, falling back to
// USER_HZ, which is 100 on all architectures supported by Go.
func getClockTicksPerSecond() uint64 {
	if auxv, err := unix.Auxv(); err == nil {
		for _, entry := range auxv {
			if entry[0] == atClkTck && entry[1] > 0 {
				return uint64(entry[1])
			}
		}
	}
	return 100
}

var clockTicksPerSecond = getClockTicksPerSecond()

// isProcessUnavailable returns true if an error that occurred while
// inspecting a process indicates that the process terminated in the
// meantime, or that procfs does not permit it to be inspected (e.g.,
// due to the hidepid= mount option). Such processes are omitted from
// the process table.
func isProcessUnavailable(err error) bool {
	return errors.Is(err, unix.ENOENT) ||
		errors.Is(err, unix.ESRCH) ||
		errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.EPERM)
}

// getProcessStartTime extracts the time at which a process was started
// from the contents of /proc/<pid>/stat. The time is relative to the
// time at which the system booted.
func getProcessStartTime(stat []byte) (time.Duration, error) {
	// The second field holds the name of the executable in
	// parentheses. As that name may itself contain spaces and
	// parentheses, the fields that follow it must be located by
	// searching for the last closing parenthesis.
	nameEnd := bytes.LastIndexByte(stat, ')')
	if nameEnd < 0 {
		return 0, status.Error(codes.Internal, "Executable name is not terminated")
	}
	fields := bytes.Fields(stat[nameEnd+1:])

	// The start time is the 22nd field, expressed in clock ticks.
	// It is the 20th field that follows the executable name.
	const startTimeIndex = 19
	if len(fields) <= startTimeIndex {
		return 0, status.Errorf(codes.Internal, "Expected at least %d fields after the executable name, while only %d are present", startTimeIndex+1, len(fields))
	}
	startTicks, err := strconv.ParseUint(string(fields[startTimeIndex]), 10, 64)
	if err != nil {
		return 0, util.StatusWrapWithCode(err, codes.Internal, "Invalid start time")
	}

	// Convert whole seconds and the remaining ticks separately, as
	// multiplying the number of ticks by the number of nanoseconds
	// per second would overflow after less than three years of
	// uptime.
	return time.Duration(startTicks/clockTicksPerSecond)*time.Second +
		time.Duration(startTicks%clockTicksPerSecond)*time.Second/time.Duration(clockTicksPerSecond), nil
}

// getProcess obtains the properties of a single process through its
// directory in procfs. The time at which the system booted is used to
// convert the start time of the process to a wall clock time.
func getProcess(procFD int, name string, pid int, bootTime time.Time) (Process, error) {
	// Open the directory of the process and obtain all properties
	// through that file descriptor. A file descriptor of a process
	// directory keeps referring to that one process, meaning that
	// the user ID and start time below belong to the same process,
	// even if its process ID is reused in the meantime.
	processFD, err := unix.Openat(procFD, name, unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return Process{}, err
	}
	defer unix.Close(processFD)

	var processStat unix.Stat_t
	if err := unix.Fstat(processFD, &processStat); err != nil {
		return Process{}, err
	}

	// The timestamps of the directory cannot be used as the
	// creation time of the process. They are set to the current
	// time when procfs instantiates the inode of the directory,
	// which happens again each time the inode has been evicted
	// from the kernel's inode cache (e.g., under memory pressure).
	// Use the start time that the kernel records for the process
	// itself, which never changes.
	statFD, err := unix.Openat(processFD, "stat", unix.O_CLOEXEC|unix.O_RDONLY, 0)
	if err != nil {
		return Process{}, err
	}
	statFile := os.NewFile(uintptr(statFD), "stat")
	stat, err := io.ReadAll(statFile)
	statFile.Close()
	if err != nil {
		return Process{}, err
	}
	startTime, err := getProcessStartTime(stat)
	if err != nil {
		return Process{}, err
	}

	return Process{
		ProcessID:    pid,
		UserID:       int(processStat.Uid),
		CreationTime: bootTime.Add(startTime),
	}, nil
}

type systemProcessTable struct{}

func (systemProcessTable) GetProcesses() ([]Process, error) {
	// The kernel reports the start times of processes relative to
	// the time at which the system booted. Determine the latter,
	// so that start times can be converted to wall clock times.
	//
	// The current time must be sampled before the time since boot.
	// A delay between the two calls then makes processes appear
	// to have been created earlier than they actually were, never
	// later. The same holds for time during which the system is
	// suspended, which the time since boot includes, while the
	// monotonic clock reading of time.Now() does not. This
	// guarantees that a process that already existed when
	// time.Now() was called at some earlier point is never
	// reported as having been created after that time. bb_runner
	// depends on this for processes that already existed when it
	// started, including itself.
	//
	// As creation times are derived from time.Now(), they carry a
	// monotonic clock reading. Comparing them against other times
	// obtained through time.Now() is therefore not affected by
	// changes to the wall clock.
	now := time.Now()
	var timeSinceBoot unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &timeSinceBoot); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to obtain the time since boot")
	}
	bootTime := now.Add(-time.Duration(timeSinceBoot.Nano()))

	// Open procfs.
	fd, err := unix.Open("/proc", unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to open /proc")
	}
	f := os.NewFile(uintptr(fd), ".")
	defer f.Close()

	// Obtain a list of all processes that are currently running.
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to obtain directory listing of /proc")
	}

	var processes []Process
	for _, name := range names {
		// Filter out non-process entries (e.g., /proc/cmdline).
		pid, err := strconv.ParseInt(name, 10, 0)
		if err != nil {
			continue
		}

		process, err := getProcess(fd, name, int(pid), bootTime)
		if isProcessUnavailable(err) {
			continue
		} else if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to obtain properties of process %d", pid)
		}
		processes = append(processes, process)
	}
	return processes, nil
}

// SystemProcessTable corresponds with the process table of the locally
// running operating system. On this operating system the information is
// extracted from procfs.
var SystemProcessTable ProcessTable = systemProcessTable{}
