package processtablecleaning

// ProcessFilterFunc is a callback that is provided to
// NewFilteringProcessTable to act as a filter function for processes
// returned by GetProcesses(). Only processes for which this callback
// function returns true are returned.
type ProcessFilterFunc func(process *Process) bool

type filteringProcessTable struct {
	base   ProcessTable
	filter ProcessFilterFunc
}

// NewFilteringProcessTable is a decorator for ProcessTable that only
// causes it to return processes matching a given filter. These are the
// processes that bb_runner should consider terminating.
func NewFilteringProcessTable(base ProcessTable, filter ProcessFilterFunc) ProcessTable {
	return &filteringProcessTable{
		base:   base,
		filter: filter,
	}
}

func (pt *filteringProcessTable) GetProcesses() ([]Process, error) {
	unfilteredProcesses, err := pt.base.GetProcesses()
	if err != nil {
		return nil, err
	}

	var filteredProcesses []Process
	for _, process := range unfilteredProcesses {
		if pt.filter(&process) {
			filteredProcesses = append(filteredProcesses, process)
		}
	}
	return filteredProcesses, nil
}
