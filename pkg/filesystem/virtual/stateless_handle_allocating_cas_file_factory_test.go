package virtual_test

import (
	"bytes"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestStatelessHandleAllocatingCASFileFactory(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseCASFileFactory := mock.NewMockCASFileFactory(ctrl)
	handleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	handleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	handleAllocation.EXPECT().AsStatelessAllocator().Return(handleAllocator)
	casFileFactory := virtual.NewStatelessHandleAllocatingCASFileFactory(baseCASFileFactory, handleAllocation)

	t.Run("NotExecutable", func(t *testing.T) {
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "bc126902a442931481d7f89552a41b1891cf06dd8d3675062eede66d104d97b4", 123)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCASFileFactory.EXPECT().LookupFile(blobDigest, false).Return(underlyingLeaf)
		wrappedLeaf := mock.NewMockNativeLeaf(ctrl)
		leafHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
		handleAllocator.EXPECT().New(gomock.Any()).DoAndReturn(func(id io.WriterTo) virtual.StatelessHandleAllocation {
			idBuf := bytes.NewBuffer(nil)
			n, err := id.WriteTo(idBuf)
			require.NoError(t, err)
			require.Equal(t, int64(78), n)
			require.Equal(t, []byte(
				// Length of digest.
				"\x4c"+
					// Digest.
					"1-bc126902a442931481d7f89552a41b1891cf06dd8d3675062eede66d104d97b4-123-hello"+
					// Executable flag.
					"\x00"), idBuf.Bytes())
			return leafHandleAllocation
		})
		leafHandleAllocation.EXPECT().AsNativeLeaf(underlyingLeaf).Return(wrappedLeaf)

		require.Equal(t, wrappedLeaf, casFileFactory.LookupFile(blobDigest, false))
	})

	t.Run("Executable", func(t *testing.T) {
		blobDigest := digest.MustNewDigest("foobar", remoteexecution.DigestFunction_MD5, "c8a4ddfcd3a5a0caf4cc1d64883df421", 456)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCASFileFactory.EXPECT().LookupFile(blobDigest, true).Return(underlyingLeaf)
		wrappedLeaf := mock.NewMockNativeLeaf(ctrl)
		leafHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
		handleAllocator.EXPECT().New(gomock.Any()).DoAndReturn(func(id io.WriterTo) virtual.StatelessHandleAllocation {
			idBuf := bytes.NewBuffer(nil)
			n, err := id.WriteTo(idBuf)
			require.NoError(t, err)
			require.Equal(t, int64(47), n)
			require.Equal(t, []byte(
				// Length of digest.
				"\x2d"+
					// Digest.
					"3-c8a4ddfcd3a5a0caf4cc1d64883df421-456-foobar"+
					// Executable flag.
					"\x01"), idBuf.Bytes())
			return leafHandleAllocation
		})
		leafHandleAllocation.EXPECT().AsNativeLeaf(underlyingLeaf).Return(wrappedLeaf)

		require.Equal(t, wrappedLeaf, casFileFactory.LookupFile(blobDigest, true))
	})
}
