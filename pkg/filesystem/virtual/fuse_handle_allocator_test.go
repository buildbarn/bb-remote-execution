package virtual_test

import (
	"bytes"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestFUSEHandleAllocator(t *testing.T) {
	ctrl := gomock.NewController(t)

	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	handleAllocator := virtual.NewFUSEHandleAllocator(randomNumberGenerator)
	attributesMask := virtual.AttributesMaskInodeNumber | virtual.AttributesMaskLinkCount | virtual.AttributesMaskSizeBytes

	removalNotifier := mock.NewMockFUSERemovalNotifier(ctrl)
	handleAllocator.RegisterRemovalNotifier(removalNotifier.Call)

	t.Run("StatefulDirectory", func(t *testing.T) {
		// Create a stateful directory. The handle that is
		// returned should add an inode number to the
		// attributes.
		baseDirectory := mock.NewMockVirtualDirectory(ctrl)

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xfccd1fc99a8c3425))
		directoryHandle := handleAllocator.New().AsStatefulDirectory(baseDirectory)

		var attr virtual.Attributes
		directoryHandle.GetAttributes(attributesMask, &attr)
		require.Equal(
			t,
			(&virtual.Attributes{}).SetInodeNumber(0xfccd1fc99a8c3425),
			&attr)

		// Removal notifications should be forwarded, to the
		// FUSE server, containing the inode number of the
		// parent directory.
		removalNotifier.EXPECT().Call(uint64(0xfccd1fc99a8c3425), path.MustNewComponent("output.o"))
		directoryHandle.NotifyRemoval(path.MustNewComponent("output.o"))

		directoryHandle.Release()
	})

	t.Run("StatelessDirectory", func(t *testing.T) {
		// Create a stateless directory and wrap it. Only an
		// inode number should be added, as the directory is
		// still responsible for providing its own link count.
		// The link count is based on the number of child
		// directories.
		baseDirectory := mock.NewMockVirtualDirectory(ctrl)
		baseDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskLinkCount|virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetLinkCount(17)
				attributes.SetSizeBytes(42)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xa44671c491369d36))
		wrappedDirectory := handleAllocator.New().AsStatelessDirectory(baseDirectory)

		var attr virtual.Attributes
		wrappedDirectory.VirtualGetAttributes(attributesMask, &attr)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0xa44671c491369d36).
				SetLinkCount(17).
				SetSizeBytes(42),
			&attr)
	})

	t.Run("StatefulNativeLeaf", func(t *testing.T) {
		// Create a stateful file and wrap it. A link count and
		// inode number should be added.
		baseLeaf := mock.NewMockNativeLeaf(ctrl)
		baseLeaf.EXPECT().VirtualGetAttributes(virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetSizeBytes(42)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xf999bb2fd22421d8))
		wrappedLeaf := handleAllocator.New().AsNativeLeaf(baseLeaf)

		var attr1 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr1)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(1).
				SetSizeBytes(42),
			&attr1)

		// Hardlinking it should cause the link count to be
		// increased.
		require.Equal(t, virtual.StatusOK, wrappedLeaf.Link())

		var attr2 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr2)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(2).
				SetSizeBytes(42),
			&attr2)

		// Unlinking it twice should cause the underlying leaf
		// node to be unlinked.
		wrappedLeaf.Unlink()
		baseLeaf.EXPECT().Unlink()
		wrappedLeaf.Unlink()

		var attr3 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr3)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(0).
				SetSizeBytes(42),
			&attr3)

		// Attempting to link it again should fail, as files
		// cannot be brought back after being unlinked.
		require.Equal(t, virtual.StatusErrStale, wrappedLeaf.Link())

		var attr4 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr4)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0xf999bb2fd22421d8).
				SetLinkCount(0).
				SetSizeBytes(42),
			&attr4)
	})

	t.Run("StatelessNativeLeaf", func(t *testing.T) {
		// Create a stateless file and wrap it. A link count and
		// inode number should be added. As the file is
		// stateless, the link count uses a placeholder value.
		//
		// The inode number of the leaf corresponds with the
		// FNV-1a hash of "Hello", using 0x6aae40a05f45b861 as
		// the offset basis.
		baseLeaf := mock.NewMockNativeLeaf(ctrl)
		baseLeaf.EXPECT().VirtualGetAttributes(virtual.AttributesMaskSizeBytes, gomock.Any()).
			Do(func(attributesMask virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetSizeBytes(123)
			}).AnyTimes()

		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x6aae40a05f45b861))
		wrappedLeaf := handleAllocator.
			New().
			AsStatelessAllocator().
			New(bytes.NewBuffer([]byte("Hello"))).
			AsNativeLeaf(baseLeaf)

		var attr1 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr1)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr1)

		// Hardlinking should have no effect.
		require.Equal(t, virtual.StatusOK, wrappedLeaf.Link())

		var attr2 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr2)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr2)

		// Unlinking also has no effect.
		wrappedLeaf.Unlink()
		wrappedLeaf.Unlink()

		var attr3 virtual.Attributes
		wrappedLeaf.VirtualGetAttributes(attributesMask, &attr3)
		require.Equal(
			t,
			(&virtual.Attributes{}).
				SetInodeNumber(0x2fac04c71c5c810f).
				SetLinkCount(virtual.StatelessLeafLinkCount).
				SetSizeBytes(123),
			&attr3)
	})
}
