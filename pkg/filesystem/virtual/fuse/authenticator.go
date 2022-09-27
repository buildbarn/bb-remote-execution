//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// Authenticator of incoming FUSE requests.
type Authenticator interface {
	Authenticate(ctx context.Context, caller *fuse.Caller) (context.Context, fuse.Status)
}
