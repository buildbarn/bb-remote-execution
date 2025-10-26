package pool_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEmptyFilePool(t *testing.T) {
	_, err := pool.EmptyFilePool.NewFile(pool.ZeroHoleSource, 0)
	require.Equal(t, err, status.Error(codes.ResourceExhausted, "Cannot create file in empty file pool"))
}
