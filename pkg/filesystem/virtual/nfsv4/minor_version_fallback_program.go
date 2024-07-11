package nfsv4

import (
	"context"

	"github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
)

type minorVersionFallbackProgram struct {
	backends []nfsv4.Nfs4Program
}

// NewMinorVersionFallbackProgram creates an NFSv4 server that forwards
// requests to a series of backends, until one of the listed servers
// accepts the minor version that is embedded in the request.
//
// This decorator can be used to launch an NFSv4 server that is capable
// of processing both NFSv4.0 and NFSv4.1 requests.
func NewMinorVersionFallbackProgram(backends []nfsv4.Nfs4Program) nfsv4.Nfs4Program {
	return &minorVersionFallbackProgram{
		backends: backends,
	}
}

func (minorVersionFallbackProgram) NfsV4Nfsproc4Null(ctx context.Context) error {
	return nil
}

func (p *minorVersionFallbackProgram) NfsV4Nfsproc4Compound(ctx context.Context, arguments *nfsv4.Compound4args) (*nfsv4.Compound4res, error) {
	for _, backend := range p.backends {
		res, err := backend.NfsV4Nfsproc4Compound(ctx, arguments)
		if err != nil || res.Status != nfsv4.NFS4ERR_MINOR_VERS_MISMATCH {
			return res, err
		}
	}

	// None of the backends support the requested version.
	return &nfsv4.Compound4res{
		Status: nfsv4.NFS4ERR_MINOR_VERS_MISMATCH,
		Tag:    arguments.Tag,
	}, nil
}
