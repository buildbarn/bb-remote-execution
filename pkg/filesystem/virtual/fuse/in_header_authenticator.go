//go:build linux
// +build linux

package fuse

import (
	"context"
	"log"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type inHeaderAuthenticator struct {
	metadataExtractor *jmespath.Expression
}

// NewInHeaderAuthenticator creates an Authenticator that obtains
// authentication metadata from an incoming FUSE request by inspecting
// the "fuse_in_header" structure that's provided by the kernel. This
// structure contains the user ID, group ID and process ID of the
// calling process.
func NewInHeaderAuthenticator(metadataExtractor *jmespath.Expression) Authenticator {
	return &inHeaderAuthenticator{
		metadataExtractor: metadataExtractor,
	}
}

func (a *inHeaderAuthenticator) Authenticate(ctx context.Context, caller *fuse.Caller) (context.Context, fuse.Status) {
	raw, err := a.metadataExtractor.Search(map[string]any{
		"uid": caller.Uid,
		"gid": caller.Gid,
		"pid": caller.Pid,
	})
	if err != nil {
		log.Print("Failed to perform authentication metadata extraction: ", err)
		return nil, fuse.EIO
	}
	authenticationMetadata, err := auth.NewAuthenticationMetadataFromRaw(raw)
	if err != nil {
		log.Print("Failed to create authentication metadata: ", err)
		return nil, fuse.EIO
	}
	return auth.NewContextWithAuthenticationMetadata(ctx, authenticationMetadata), fuse.OK
}
