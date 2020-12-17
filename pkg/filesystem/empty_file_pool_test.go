package filesystem_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEmptyFilePool(t *testing.T) {
	_, err := filesystem.EmptyFilePool.NewFile()
	require.Equal(t, err, status.Error(codes.ResourceExhausted, "Cannot create file in empty file pool"))
}
