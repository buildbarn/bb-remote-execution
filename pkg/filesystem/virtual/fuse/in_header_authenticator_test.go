//go:build darwin || linux
// +build darwin linux

package fuse_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/fuse"
	"github.com/buildbarn/bb-storage/pkg/auth"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"
)

func TestInHeaderAuthenticator(t *testing.T) {
	authenticator := fuse.NewInHeaderAuthenticator(jmespath.MustCompile("{\"public\": @}"))

	ctxWithMetadata, s := authenticator.Authenticate(context.Background(), &go_fuse.Caller{
		Owner: go_fuse.Owner{
			Uid: 1000,
			Gid: 100,
		},
		Pid: 10847,
	})
	require.Equal(t, go_fuse.OK, s)
	require.Equal(t, map[string]any{
		"public": map[string]any{
			"uid": 1000.0,
			"gid": 100.0,
			"pid": 10847.0,
		},
	}, auth.AuthenticationMetadataFromContext(ctxWithMetadata).GetRaw())
}
