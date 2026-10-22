//go:build linux
// +build linux

package cleaner

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type systemProcessTable struct{}

func (systemProcessTable) GetProcesses() ([]Process, error) {
	// Open procfs.
	fd, err := unix.Open("/proc", unix.O_DIRECTORY|unix.O_RDONLY, 0)
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

		// Stat process directory entries to obtain the user ID.
		// Their timestamps cannot be used as the process creation
		// time, as procfs sets them to the current time whenever
		// the inode is instantiated, which may happen again after
		// it is evicted from the inode cache.
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to stat process %d", pid)
		}
		creationTime, err := getCreationTime(fd, name)
		if os.IsNotExist(err) || err == unix.ESRCH {
			continue
		} else if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to obtain creation time of process %d", pid)
		}
		processes = append(processes, Process{
			ProcessID:    int(pid),
			UserID:       int(stat.Uid),
			CreationTime: creationTime,
		})
	}
	return processes, nil
}

// clockTicksPerSecond is the unit of the start time in
// /proc/[pid]/stat. The kernel exposes it to userspace as USER_HZ, which
// is 100 on all architectures supported by Linux.
const clockTicksPerSecond = 100

// getCreationTime computes the creation time of a process from the start
// time in /proc/[pid]/stat, which is expressed in clock ticks since boot.
func getCreationTime(procFD int, name string) (time.Time, error) {
	fd, err := unix.Openat(procFD, name+"/stat", unix.O_RDONLY, 0)
	if err != nil {
		return time.Time{}, err
	}
	f := os.NewFile(uintptr(fd), name+"/stat")
	data, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return time.Time{}, err
	}

	// The command name is enclosed in parentheses and may contain
	// spaces and parentheses itself. The start time is the 22nd
	// field, which is the 20th field following the command name.
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return time.Time{}, status.Error(codes.Internal, "Command name is not terminated")
	}
	fields := bytes.Fields(data[i+1:])
	if len(fields) < 20 {
		return time.Time{}, status.Errorf(codes.Internal, "Expected at least 20 fields following the command name, while %d were found", len(fields))
	}
	startTicks, err := strconv.ParseUint(string(fields[19]), 10, 64)
	if err != nil {
		return time.Time{}, util.StatusWrapWithCode(err, codes.Internal, "Invalid start time")
	}

	// Convert the start time to wall clock time by comparing it
	// against the time elapsed since boot.
	var sinceBoot unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &sinceBoot); err != nil {
		return time.Time{}, err
	}
	now := time.Now()
	started := time.Duration(startTicks) * time.Second / clockTicksPerSecond
	return now.Add(started - time.Duration(sinceBoot.Nano())), nil
}

// SystemProcessTable corresponds with the process table of the locally
// running operating system. On this operating system the information is
// extracted from procfs.
var SystemProcessTable ProcessTable = systemProcessTable{}
