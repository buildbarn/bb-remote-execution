package fuse_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/stretchr/testify/require"
)

func TestDeterministicInodeNumberTree(t *testing.T) {
	t.Run("Base", func(t *testing.T) {
		// Root inode number based on the initial state.
		require.Equal(
			t,
			uint64(0x0807060504030201),
			fuse.DeterministicInodeNumberTree.Get())
	})

	t.Run("AddString", func(t *testing.T) {
		// SHA-256(initial state + "Hello") =
		// e7f65d8995590ffa1803708786acc9238a553162a4210129e1401b252024b6d1
		require.Equal(
			t,
			uint64(0xfa0f5995895df6e7),
			fuse.DeterministicInodeNumberTree.AddString("Hello").Get())
	})

	t.Run("AddUint64", func(t *testing.T) {
		// SHA-256(initial state + 0x12 + 0x34 + ... + 0xef) =
		// 858ceb11824392948ef77230e8fa73f6a8f2cb5d0c26144f70fe53ea674e023f
		require.Equal(
			t,
			uint64(0x9492438211eb8c85),
			fuse.DeterministicInodeNumberTree.AddUint64(0xefcdab9078563412).Get())
	})
}
