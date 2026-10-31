package cas

import (
	"bytes"
	"context"

	"github.com/buildbarn/bb-storage/pkg/capabilities"
	bb_cas "github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/proto"
)

// ProtoPutter stores Protobuf messages in the Content Addressable
// Storage (CAS). The digest of the stored message is returned, so that
// the object may be referenced.
type ProtoPutter interface {
	PutProto(ctx context.Context, message proto.Message, digestFunction digest.Function) (digest.Digest, error)
}

// casProtoPutter is a ProtoPutter that stores messages as blobs in the
// CAS.
type casProtoPutter struct {
	readerPutter         bb_cas.ReaderPutter
	cdcParametersFetcher capabilities.CDCParametersFetcher
}

// NewProtoPutter returns a ProtoPutter that stores messages in the CAS
// using the provided ReaderPutter.
func NewProtoPutter(readerPutter bb_cas.ReaderPutter, cdcParametersFetcher capabilities.CDCParametersFetcher) ProtoPutter {
	return &casProtoPutter{
		readerPutter:         readerPutter,
		cdcParametersFetcher: cdcParametersFetcher,
	}
}

func (p *casProtoPutter) PutProto(ctx context.Context, message proto.Message, digestFunction digest.Function) (digest.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}
	digestGenerator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := digestGenerator.Write(data); err != nil {
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()
	params, err := p.cdcParametersFetcher.FetchCDCParameters(ctx, digestFunction.GetInstanceName())
	if err != nil {
		return digest.BadDigest, util.StatusWrap(err, "Could not determine cdc parameters")
	}
	if err := p.readerPutter.PutReaderAt(ctx, blobDigest, bytes.NewReader(data), params); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
