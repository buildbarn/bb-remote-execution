//go:build linux

package runner

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"google.golang.org/protobuf/types/known/durationpb"
)

type cgroupResourceUsageReader struct {
	cgroupPath string

	// memory.events counters
	eventsLow          int64
	eventsHigh         int64
	eventsMax          int64
	eventsOOM          int64
	eventsOOMKill      int64
	eventsOOMGroupKill int64

	// PSI total values
	psiMemorySomeUS int64
	psiMemoryFullUS int64
	psiCPUSomeUS    int64
	psiCPUFullUS    int64
	psiIOSomeUS     int64
	psiIOFullUS     int64

	memoryPeakFile *os.File
}

// NewCgroupResourceUsageReaderFromPath creates a reader that samples resource
// usage counters from the cgroup v2 directory at cgroupPath.
func NewCgroupResourceUsageReaderFromPath(cgroupPath string) (CgroupResourceUsageReader, error) {
	events, err := readCgroupKeyValues(filepath.Join(cgroupPath, "memory.events"))
	if err != nil {
		return nil, fmt.Errorf("failed to read memory.events: %w", err)
	}

	memorySome, memoryFull, err := parsePSITotals(filepath.Join(cgroupPath, "memory.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read memory.pressure: %w", err)
	}
	cpuSome, cpuFull, err := parsePSITotals(filepath.Join(cgroupPath, "cpu.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read cpu.pressure: %w", err)
	}
	ioSome, ioFull, err := parsePSITotals(filepath.Join(cgroupPath, "io.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read io.pressure: %w", err)
	}

	// memory.peak is optional. If it is unavailable or cannot be reset/read,
	// MemoryPeak remains 0 to indicate that no peak data was collected.
	memoryPeakFile := openAndResetCgroupMemoryPeak(filepath.Join(cgroupPath, "memory.peak"))
	reader := &cgroupResourceUsageReader{
		cgroupPath: cgroupPath,

		eventsLow:          events["low"],
		eventsHigh:         events["high"],
		eventsMax:          events["max"],
		eventsOOM:          events["oom"],
		eventsOOMKill:      events["oom_kill"],
		eventsOOMGroupKill: events["oom_group_kill"],

		psiMemorySomeUS: memorySome,
		psiMemoryFullUS: memoryFull,
		psiCPUSomeUS:    cpuSome,
		psiCPUFullUS:    cpuFull,
		psiIOSomeUS:     ioSome,
		psiIOFullUS:     ioFull,

		memoryPeakFile: memoryPeakFile,
	}
	return reader, nil
}

func (r *cgroupResourceUsageReader) Close() error {
	if r == nil || r.memoryPeakFile == nil {
		return nil
	}
	return r.memoryPeakFile.Close()
}

func (r *cgroupResourceUsageReader) Read() (*resourceusage.CgroupResourceUsage, error) {
	events, err := readCgroupKeyValues(filepath.Join(r.cgroupPath, "memory.events"))
	if err != nil {
		return nil, fmt.Errorf("failed to read memory.events: %w", err)
	}

	memorySome, memoryFull, err := parsePSITotals(filepath.Join(r.cgroupPath, "memory.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read memory.pressure: %w", err)
	}
	cpuSome, cpuFull, err := parsePSITotals(filepath.Join(r.cgroupPath, "cpu.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read cpu.pressure: %w", err)
	}
	ioSome, ioFull, err := parsePSITotals(filepath.Join(r.cgroupPath, "io.pressure"))
	if err != nil {
		return nil, fmt.Errorf("failed to read io.pressure: %w", err)
	}

	memoryPeak := readCgroupMemoryPeak(r.memoryPeakFile)

	return &resourceusage.CgroupResourceUsage{
		MemoryEventsLow:          events["low"] - r.eventsLow,
		MemoryEventsHigh:         events["high"] - r.eventsHigh,
		MemoryEventsMax:          events["max"] - r.eventsMax,
		MemoryEventsOom:          events["oom"] - r.eventsOOM,
		MemoryEventsOomKill:      events["oom_kill"] - r.eventsOOMKill,
		MemoryEventsOomGroupKill: events["oom_group_kill"] - r.eventsOOMGroupKill,

		MemoryPeak: memoryPeak,

		MemoryPressureSomeTotal: microsecondsDuration(memorySome - r.psiMemorySomeUS),
		MemoryPressureFullTotal: microsecondsDuration(memoryFull - r.psiMemoryFullUS),
		CpuPressureSomeTotal:    microsecondsDuration(cpuSome - r.psiCPUSomeUS),
		CpuPressureFullTotal:    microsecondsDuration(cpuFull - r.psiCPUFullUS),
		IoPressureSomeTotal:     microsecondsDuration(ioSome - r.psiIOSomeUS),
		IoPressureFullTotal:     microsecondsDuration(ioFull - r.psiIOFullUS),
	}, nil
}

func microsecondsDuration(microseconds int64) *durationpb.Duration {
	return durationpb.New(time.Duration(microseconds) * time.Microsecond)
}

func openAndResetCgroupMemoryPeak(path string) *os.File {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil
	}
	// Linux scopes memory.peak reset state to the file descriptor used for
	// the write, so keep this descriptor open until the action finishes.
	if _, err := f.Write([]byte("1")); err != nil {
		f.Close()
		return nil
	}
	return f
}

func readCgroupMemoryPeak(f *os.File) int64 {
	if f == nil {
		return 0
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return 0
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// readCgroupKeyValues parses a cgroup file with key-value lines
// (e.g., memory.events, memory.stat). Each line has the form "key value".
func readCgroupKeyValues(path string) (map[string]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]int64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		result[parts[0]] = v
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// parsePSITotals parses a PSI pressure file and returns the total
// stall microseconds for the "some" and "full" lines.
// Format: some avg10=0.00 avg60=0.00 avg300=0.00 total=12345
func parsePSITotals(path string) (someUS, fullUS int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		prefix := fields[0]
		var total int64
		foundTotal := false
		for _, field := range fields[1:] {
			if strings.HasPrefix(field, "total=") {
				var err error
				total, err = strconv.ParseInt(field[len("total="):], 10, 64)
				if err != nil {
					return 0, 0, err
				}
				foundTotal = true
				break
			}
		}
		if !foundTotal {
			return 0, 0, fmt.Errorf("missing total field in %q", line)
		}
		switch prefix {
		case "some":
			someUS = total
		case "full":
			fullUS = total
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	return someUS, fullUS, nil
}

// ResolveCurrentCgroupfsPath resolves the cgroup v2 filesystem directory of
// the current process.
func ResolveCurrentCgroupfsPath() (string, error) {
	return ResolveCurrentCgroupfsPathFromProcFiles("/proc/self/cgroup", "/proc/self/mountinfo")
}

// ResolveCurrentCgroupfsPathFromProcFiles reads procCgroupPath and
// procMountInfoPath to resolve the cgroup v2 filesystem directory of the
// current process.
func ResolveCurrentCgroupfsPathFromProcFiles(procCgroupPath, procMountInfoPath string) (string, error) {
	currentCgroupPath, err := readCurrentCgroupRelativePath(procCgroupPath)
	if err != nil {
		return "", err
	}
	return resolveCgroupPathFromMountInfo(procMountInfoPath, currentCgroupPath)
}

func resolveCgroupPathFromMountInfo(path, currentCgroupPath string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read cgroup mount information: %w", err)
	}
	currentCgroupPath = cleanCgroupPath(currentCgroupPath)
	var bestMountPoint, bestRoot string
	for _, line := range strings.Split(string(data), "\n") {
		separator := strings.Index(line, " - ")
		if separator < 0 {
			continue
		}
		mountFields := strings.Fields(line[:separator])
		filesystemFields := strings.Fields(line[separator+3:])
		if len(mountFields) < 5 || len(filesystemFields) < 1 || filesystemFields[0] != "cgroup2" {
			continue
		}
		root := cleanCgroupPath(unescapeMountInfoField(mountFields[3]))
		if !isCgroupPathPrefix(root, currentCgroupPath) {
			continue
		}
		if len(root) <= len(bestRoot) {
			continue
		}
		bestRoot = root
		mountPoint := filepath.Clean(unescapeMountInfoField(mountFields[4]))
		relativePath, err := filepath.Rel(root, currentCgroupPath)
		if err != nil {
			continue
		}
		bestMountPoint = filepath.Join(mountPoint, relativePath)
	}
	if bestMountPoint == "" {
		return "", fmt.Errorf("cgroup v2 mount containing current cgroup %q not found in %s", currentCgroupPath, path)
	}
	return bestMountPoint, nil
}

func cleanCgroupPath(path string) string {
	path = filepath.Clean(path)
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(string(filepath.Separator) + path)
}

func isCgroupPathPrefix(root, path string) bool {
	relativePath, err := filepath.Rel(root, path)
	return err == nil &&
		relativePath != ".." &&
		!strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) &&
		!filepath.IsAbs(relativePath)
}

// /proc/self/mountinfo encodes whitespace and backslash in path fields using
// octal escape sequences.
func unescapeMountInfoField(field string) string {
	replacer := strings.NewReplacer(
		`\011`, "\t",
		`\012`, "\n",
		`\040`, " ",
		`\134`, `\`,
	)
	return replacer.Replace(field)
}

func readCurrentCgroupRelativePath(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read current cgroup: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
			return filepath.Clean(parts[2]), nil
		}
	}
	return "", fmt.Errorf("cgroup v2 entry not found in %s", path)
}
