//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type allowAuthenticator struct{}

func (allowAuthenticator) Authenticate(ctx context.Context, caller *fuse.Caller) (context.Context, fuse.Status) {
	return ctx, fuse.OK
}

// AllowAuthenticator is an implementation of Authenticator that simply
// permits all incoming requests. No authentication metadata is attached
// to the context.
var AllowAuthenticator Authenticator = allowAuthenticator{}
