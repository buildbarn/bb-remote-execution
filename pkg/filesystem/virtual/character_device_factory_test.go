package virtual_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleAllocatingCharacterDeviceFactory(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseCharacterDeviceFactory := mock.NewMockCharacterDeviceFactory(ctrl)
	rootHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	var handleResolver virtual.HandleResolver
	rootHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).
		DoAndReturn(func(hr virtual.HandleResolver) virtual.ResolvableHandleAllocator {
			handleResolver = hr
			return handleAllocator
		})
	characterDeviceFactory := virtual.NewHandleAllocatingCharacterDeviceFactory(
		baseCharacterDeviceFactory,
		rootHandleAllocation)

	t.Run("Lookup", func(t *testing.T) {
		// Look up /dev/null (on Linux: major 1, minor 3).
		deviceNumber := filesystem.NewDeviceNumberFromMajorMinor(1, 3)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCharacterDeviceFactory.EXPECT().LookupCharacterDevice(deviceNumber).Return(underlyingLeaf)
		wrappedLeaf := mock.NewMockNativeLeaf(ctrl)
		leafHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
		handleAllocator.EXPECT().New(gomock.Any()).
			DoAndReturn(func(id io.WriterTo) virtual.ResolvableHandleAllocation {
				idBuf := bytes.NewBuffer(nil)
				n, err := id.WriteTo(idBuf)
				require.NoError(t, err)
				require.Equal(t, int64(2), n)
				require.Equal(t, []byte{1, 3}, idBuf.Bytes())
				return leafHandleAllocation
			})
		leafHandleAllocation.EXPECT().AsNativeLeaf(underlyingLeaf).Return(wrappedLeaf)

		require.Equal(t, wrappedLeaf, characterDeviceFactory.LookupCharacterDevice(deviceNumber))
	})

	t.Run("ResolverEmpty", func(t *testing.T) {
		// An empty file handle should not resolve.
		_, _, s := handleResolver(bytes.NewBuffer(nil))
		require.Equal(t, virtual.StatusErrBadHandle, s)
	})

	t.Run("ResolverSingleNumber", func(t *testing.T) {
		// Only provided a major number.
		_, _, s := handleResolver(bytes.NewBuffer([]byte{1}))
		require.Equal(t, virtual.StatusErrBadHandle, s)
	})

	t.Run("ResolverTwoNumbers", func(t *testing.T) {
		// Provided both a major and minor number.
		deviceNumber := filesystem.NewDeviceNumberFromMajorMinor(1, 3)
		underlyingLeaf := mock.NewMockNativeLeaf(ctrl)
		baseCharacterDeviceFactory.EXPECT().LookupCharacterDevice(deviceNumber).Return(underlyingLeaf)
		wrappedLeaf := mock.NewMockNativeLeaf(ctrl)
		leafHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
		handleAllocator.EXPECT().New(gomock.Any()).
			DoAndReturn(func(id io.WriterTo) virtual.ResolvableHandleAllocation {
				idBuf := bytes.NewBuffer(nil)
				n, err := id.WriteTo(idBuf)
				require.NoError(t, err)
				require.Equal(t, int64(2), n)
				require.Equal(t, []byte{1, 3}, idBuf.Bytes())
				return leafHandleAllocation
			})
		leafHandleAllocation.EXPECT().AsNativeLeaf(underlyingLeaf).Return(wrappedLeaf)

		_, actualLeaf, s := handleResolver(bytes.NewBuffer([]byte{1, 3}))
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, wrappedLeaf, actualLeaf)
	})
}
