// +build linux

package processtablecleaning

import (
	"os"
	"strconv"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
)

type systemProcessTable struct{}

func (pt systemProcessTable) GetProcesses() ([]Process, error) {
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

		// Stat process directory entries to obtain user ID and
		// creation time.
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to stat process %d", pid)
		}
		processes = append(processes, Process{
			ProcessID:    int(pid),
			UserID:       int(stat.Uid),
			CreationTime: time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)),
		})
	}
	return processes, nil
}

// SystemProcessTable corresponds with the process table of the locally
// running operating system. On this operating system the information is
// extracted from procfs.
var SystemProcessTable ProcessTable = systemProcessTable{}
