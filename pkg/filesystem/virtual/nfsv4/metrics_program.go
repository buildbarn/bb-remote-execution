package nfsv4

import (
	"context"
	"sync"

	"github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	programPrometheusMetrics sync.Once

	programCompoundOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "nfsv4",
			Name:      "program_compound_operations_total",
			Help:      "Number of operations provided as part of calls to NFSv4 COMPOUND.",
		},
		[]string{"operation", "status"})
	programCompoundOperationsOK map[nfsv4.NfsOpnum4]prometheus.Counter
)

type metricsProgram struct {
	nfsv4.Nfs4Program
}

// NewMetricsProgram creates a decorator for nfsv4.Nfs4Program that
// exposes Prometheus metrics for all compound operations called.
//
// Right now it only provides counters for the number of operations
// called. Timing of operation is not exposed, as it can only be
// computed at the procedure level, which isn't meaningful in practice.
func NewMetricsProgram(base nfsv4.Nfs4Program) nfsv4.Nfs4Program {
	programPrometheusMetrics.Do(func() {
		prometheus.MustRegister(programCompoundOperations)

		// Already create counters for all of the operations
		// where status is NFS4_OK. This allows us to skip calls
		// to WithLabelValues() in the common case.
		programCompoundOperationsOK = map[nfsv4.NfsOpnum4]prometheus.Counter{}
		for code, name := range nfsv4.NfsOpnum4_name {
			programCompoundOperationsOK[code] = programCompoundOperations.WithLabelValues(name, nfsv4.Nfsstat4_name[nfsv4.NFS4_OK])
		}
	})

	return &metricsProgram{
		Nfs4Program: base,
	}
}

func (p *metricsProgram) NfsV4Nfsproc4Compound(ctx context.Context, arguments *nfsv4.Compound4args) (*nfsv4.Compound4res, error) {
	compoundRes, err := p.Nfs4Program.NfsV4Nfsproc4Compound(ctx, arguments)
	if err != nil {
		return nil, err
	}

	for i, res := range compoundRes.Resarray {
		operation := res.GetResop()
		status := nfsv4.NFS4_OK
		if i == len(compoundRes.Resarray)-1 {
			status = compoundRes.Status
		}
		if status == nfsv4.NFS4_OK {
			if counter, ok := programCompoundOperationsOK[operation]; ok {
				// Fast path: a known operation has succeeded.
				counter.Inc()
				continue
			}
		}

		// Slow path: either the operation is not known, or it failed.
		operationStr, ok := nfsv4.NfsOpnum4_name[operation]
		if !ok {
			operationStr = "UNKNOWN"
		}
		statusStr, ok := nfsv4.Nfsstat4_name[status]
		if !ok {
			statusStr = "UNKNOWN"
		}
		programCompoundOperations.WithLabelValues(operationStr, statusStr).Inc()
	}

	return compoundRes, nil
}
