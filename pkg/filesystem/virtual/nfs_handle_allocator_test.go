package virtual_test

import (
	"bytes"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNFSHandleAllocator(t *testing.T) {
	ctrl := gomock.NewController(t)

	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	handleAllocator := virtual.NewNFSHandleAllocator(randomNumberGenerator)
	attributesMask := virtual.AttributesMaskChangeID |
		virtual.AttributesMaskFileHandle |
		virtual.AttributesMaskInodeNumber |
		virtual.AttributesMaskLinkCount |
		virtual.AttributesMaskSizeBytes

	t.Run("StatefulDirectory", func(t *testing.T) {
		// Create a stateful directory. The handle that is
		// returned should add a file handle and inode number to
		// the attributes.
		baseDirectory := mock.NewMockVirtualDirectory(ctrl)

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xfccd1fc99a8c3425))
		directoryHandle := handleAllocator.New().AsStatefulDirectory(baseDirectory)

		fileHandle := []byte{0x25, 0x34, 0x8c, 0x9a, 0xc9, 0x1f, 0xcd, 0xfc}
		var attr virtual.Attributes
		directoryHandle.GetAttributes(attributesMask, &attr)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xfccd1fc99a8c3425),
			&attr)

		// The directory should be resolvable.
		resolvedDirectory, resolvedLeaf, s := handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, baseDirectory, resolvedDirectory)
		require.Nil(t, resolvedLeaf)

		// After releasing the directory, it should no longer be
		// resolvable.
		directoryHandle.Release()

		_, _, s = handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusErrStale, s)
	})

	t.Run("StatelessDirectory", func(t *testing.T) {
		// Create a stateless directory and wrap it. Only a file
		// handle and inode number should be added, as the
		// directory is still responsible for providing its own
		// link count. The link count is based on the number of
		// child directories.
		baseDirectory := mock.NewMockVirtualDirectory(ctrl)
		baseDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskChangeID|virtual.AttributesMaskLinkCount|virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetChangeID(0)
				attributes.SetLinkCount(17)
				attributes.SetSizeBytes(42)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xa44671c491369d36))
		wrappedDirectory := handleAllocator.New().AsStatelessDirectory(baseDirectory)

		fileHandle := []byte{0x36, 0x9d, 0x36, 0x91, 0xc4, 0x71, 0x46, 0xa4}
		var attr virtual.Attributes
		wrappedDirectory.VirtualGetAttributes(attributesMask, &attr)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xa44671c491369d36).
				SetLinkCount(17).
				SetSizeBytes(42),
			&attr)

		// The directory should be resolvable.
		resolvedDirectory, resolvedLeaf, s := handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, wrappedDirectory, resolvedDirectory)
		require.Nil(t, resolvedLeaf)
	})

	t.Run("StatefulNativeLeaf", func(t *testing.T) {
		// Create a stateful file and wrap it. A file handle, link
		// count and inode number should be added.
		baseLeaf := mock.NewMockNativeLeaf(ctrl)
		baseLeaf.EXPECT().VirtualGetAttributes(virtual.AttributesMaskChangeID|virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetChangeID(7)
				attributes.SetSizeBytes(42)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xf999bb2fd22421d8))
		wrappedLeaf := handleAllocator.New().AsNativeLeaf(baseLeaf)

		fileHandle := []byte{0xd8, 0x21, 0x24, 0xd2, 0x2f, 0xbb, 0x99, 0xf9}
		var attr1 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr1)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(7).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(1).
				SetSizeBytes(42),
			&attr1)

		// The leaf should be resolvable.
		resolvedDirectory, resolvedLeaf, s := handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusOK, s)
		require.Nil(t, resolvedDirectory)
		require.Equal(t, wrappedLeaf, resolvedLeaf)

		// Hardlinking it should cause the link count to be
		// increased.
		require.Equal(t, virtual.StatusOK, wrappedLeaf.Link())

		var attr2 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr2)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(8).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(2).
				SetSizeBytes(42),
			&attr2)

		// Unlinking it twice should cause the underlying leaf
		// node to be unlinked. It should then no longer be
		// resolvable.
		wrappedLeaf.Unlink()
		baseLeaf.EXPECT().Unlink()
		wrappedLeaf.Unlink()

		var attr3 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr3)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(10).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(0).
				SetSizeBytes(42),
			&attr3)

		_, _, s = handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusErrStale, s)

		// Attempting to link it again should fail, as files
		// cannot be brought back after being unlinked.
		require.Equal(t, virtual.StatusErrStale, wrappedLeaf.Link())

		var attr4 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr4)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(10).
				SetFileHandle(fileHandle).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(0).
				SetSizeBytes(42),
			&attr4)
	})

	t.Run("StatelessNativeLeaf", func(t *testing.T) {
		// Create a stateless file and wrap it. A link count and
		// inode number should be added. As the file is
		// stateless, the reported link count uses a placeholder
		// value. It does have a link count under the hood to
		// determine when the file no longer needs to be
		// resolvable.
		//
		// The inode number of the leaf corresponds with the
		// FNV-1a hash of "Hello", using 0x6aae40a05f45b861 as
		// the offset basis.
		baseLeaf := mock.NewMockNativeLeaf(ctrl)
		baseLeaf.EXPECT().VirtualGetAttributes(virtual.AttributesMaskChangeID|virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetChangeID(0)
				attributes.SetSizeBytes(123)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x6aae40a05f45b861))
		wrappedLeaf := handleAllocator.
			New().
			AsStatelessAllocator().
			New(bytes.NewBuffer([]byte("Hello"))).
			AsNativeLeaf(baseLeaf)

		fileHandle := []byte{0x0f, 0x81, 0x5c, 0x1c, 0xc7, 0x04, 0xac, 0x2f}
		var attr1 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr1)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileHandle(fileHandle).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr1)

		// The leaf should be resolvable.
		resolvedDirectory, resolvedLeaf, s := handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusOK, s)
		require.Nil(t, resolvedDirectory)
		require.Equal(t, wrappedLeaf, resolvedLeaf)

		// Hardlinking should have no visible effect, even
		// though a link count under the hood is adjusted.
		require.Equal(t, virtual.StatusOK, wrappedLeaf.Link())

		var attr2 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr2)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileHandle(fileHandle).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr2)

		// Unlinking it twice should cause the underlying leaf
		// node to be unlinked. It should then no longer be
		// resolvable.
		wrappedLeaf.Unlink()
		baseLeaf.EXPECT().Unlink()
		wrappedLeaf.Unlink()

		var attr3 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr3)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileHandle(fileHandle).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr3)

		_, _, s = handleAllocator.ResolveHandle(bytes.NewBuffer(fileHandle))
		require.Equal(t, virtual.StatusErrStale, s)

		// Attempting to link it again should fail, as files
		// cannot be brought back after being unlinked.
		require.Equal(t, virtual.StatusErrStale, wrappedLeaf.Link())

		var attr4 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr4)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileHandle(fileHandle).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr4)
	})
}
