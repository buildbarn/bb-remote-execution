package cleaner

import (
	"time"
)

// Process running on the operating system. This type contains a subset
// of the information normally displayed by tools such as ps and top.
type Process struct {
	ProcessID    int
	UserID       int
	CreationTime time.Time
}

// ProcessTable is an interface for extracting a list of processes
// running on the operating system.
type ProcessTable interface {
	GetProcesses() ([]Process, error)
}
