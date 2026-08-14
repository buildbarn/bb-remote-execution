package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type casMessageReader[T proto.Message] struct {
	contentAddressableStorage cdc.ContentAddressableStorage
	maximumMessageSizeBytes   int
}

func NewCASMessageReader[T proto.Message](contentAddressableStorage cdc.ContentAddressableStorage, maximumMessageSizeBytes int) storage.MessageReader[T] {
	return &casMessageReader[T]{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (r *casMessageReader[T]) ReadMessage(ctx context.Context, d digest.Digest, message T) (T, error) {
	var zero T
	if d.GetSizeBytes() > int64(r.maximumMessageSizeBytes) {
		return zero, status.Errorf(codes.InvalidArgument, "Message size %d exceeds maximum allowed size %d", d.GetSizeBytes(), r.maximumMessageSizeBytes)
	}
	return cdc.GetProto(ctx, r.contentAddressableStorage, d, message)
}
