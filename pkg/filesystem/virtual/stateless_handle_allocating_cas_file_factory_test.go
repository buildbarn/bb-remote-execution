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
		baseCASFileFactory.EXPECT().LookupFile(
			blobDigest,
			/* isExecutable = */ false,
			/* readMonitor = */ nil,
		).Return(underlyingLeaf)
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

		require.Equal(t, wrappedLeaf, casFileFactory.LookupFile(blobDigest, false, nil))
	})

	t.Run("Executable", func(t *testing.T) {
		blobDigest := digest.MustNewDigest("foobar", remoteexecution.DigestFunction_MD5, "c8a4ddfcd3a5a0caf4cc1d64883df421", 456)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCASFileFactory.EXPECT().LookupFile(
			blobDigest,
			/* isExecutable = */ true,
			/* readMonitor = */ nil,
		).Return(underlyingLeaf)
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

		require.Equal(t, wrappedLeaf, casFileFactory.LookupFile(blobDigest, true, nil))
	})

	t.Run("WithReadMonitor", func(t *testing.T) {
		// Create a CAS file that has a read monitor installed.
		// This should cause the returned file to be wrapped
		// twice: once to intercept VirtualRead() calls, and
		// once by the HandleAllocator.
		blobDigest := digest.MustNewDigest("foobar", remoteexecution.DigestFunction_MD5, "1234fc8071156282a346e0563ef92b6f", 123)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCASFileFactory.EXPECT().LookupFile(
			blobDigest,
			/* isExecutable = */ true,
			/* readMonitor = */ nil,
		).Return(underlyingLeaf)
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
					"3-1234fc8071156282a346e0563ef92b6f-123-foobar"+
					// Executable flag.
					"\x01"), idBuf.Bytes())
			return leafHandleAllocation
		})
		var monitoringLeaf virtual.NativeLeaf
		leafHandleAllocation.EXPECT().AsNativeLeaf(gomock.Any()).DoAndReturn(func(leaf virtual.NativeLeaf) virtual.NativeLeaf {
			monitoringLeaf = leaf
			return wrappedLeaf
		})
		fileReadMonitor := mock.NewMockFileReadMonitor(ctrl)

		require.Equal(t, wrappedLeaf, casFileFactory.LookupFile(blobDigest, true, fileReadMonitor.Call))

		// Reading the file's contents should cause it to be reported
		// as being read. This should only happen just once.
		fileReadMonitor.EXPECT().Call()
		underlyingLeaf.EXPECT().VirtualRead(gomock.Len(5), uint64(0)).
			DoAndReturn(func(buf []byte, off uint64) (int, bool, virtual.Status) {
				copy(buf, "Hello")
				return 5, false, virtual.StatusOK
			}).
			Times(10)

		for i := 0; i < 10; i++ {
			var buf [5]byte
			n, eof, s := monitoringLeaf.VirtualRead(buf[:], 0)
			require.Equal(t, 5, n)
			require.False(t, eof)
			require.Equal(t, virtual.StatusOK, s)
		}
	})
}
