package cas_test

import (
	"context"
	"fmt"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCachingDirectoryFetcherGetDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseDirectoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	evictionSet := mock.NewMockCachingDirectoryFetcherEvictionSet(ctrl)
	directoryFetcher := cas.NewCachingDirectoryFetcher(baseDirectoryFetcher, digest.KeyWithoutInstance, 10, 1000, evictionSet)

	t.Run("IOError", func(t *testing.T) {
		// Errors from underlying storage should be propagated.
		directoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "756b15c8f94b519e96135dcfde0e58c5", 50)

		baseDirectoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(nil, status.Error(codes.Internal, "I/O error"))

		_, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("Success", func(t *testing.T) {
		directoryDigests := []digest.Digest{
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "dae613d971e8649f28bf07b0d8dfefa8", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7c9a2912bf38c338baa07a6df966eabf", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7c57a169737a51b43d9853874ef39854", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bd9c8a8f4618436dad0f3d1164ff54fc", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "2b80e2292d40e6664eb458bb0d906d1e", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "b8168c5e6bcb299c61d46c7163b16216", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "a00535d5f1ce3b1257baa70f4fcac762", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "d66cc130b6436e52a503249a7cf39175", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "557d4a5911854ecd5d1fba42cce80960", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "ca62de38c5058d8f52a2f6f3eed0efdc", 100),
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "660cc65d02dc9b676d48f8ac50691a05", 100),
		}

		// Insert ten directories into the cache.
		for i, directoryDigest := range directoryDigests[:10] {
			directoryToInsert := &remoteexecution.Directory{
				Symlinks: []*remoteexecution.SymlinkNode{
					{
						Name:   "symlink",
						Target: fmt.Sprintf("target-%d", i),
					},
				},
			}
			baseDirectoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(directoryToInsert, nil)
			evictionSet.EXPECT().Insert(cas.CachingDirectoryFetcherKey{
				DigestKey:  directoryDigest.GetKey(digest.KeyWithoutInstance),
				IsTreeRoot: false,
			})

			directory, err := directoryFetcher.GetDirectory(ctx, directoryDigest)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, directoryToInsert, directory)
		}

		// It should be possible to read back one of the ten
		// entries above without incurring any additional I/O.
		evictionSet.EXPECT().Touch(cas.CachingDirectoryFetcherKey{
			DigestKey:  directoryDigests[7].GetKey(digest.KeyWithoutInstance),
			IsTreeRoot: false,
		})

		directory, err := directoryFetcher.GetDirectory(ctx, directoryDigests[7])
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target-7",
				},
			},
		}, directory)

		// Because the cache is saturated at this point,
		// inserting another element should cause one of the
		// existing entries to be removed.
		directoryToInsert := &remoteexecution.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target-10",
				},
			},
		}
		baseDirectoryFetcher.EXPECT().GetDirectory(ctx, directoryDigests[10]).Return(directoryToInsert, nil)
		evictionSet.EXPECT().Peek().Return(cas.CachingDirectoryFetcherKey{
			DigestKey:  directoryDigests[3].GetKey(digest.KeyWithoutInstance),
			IsTreeRoot: false,
		})
		evictionSet.EXPECT().Remove()
		evictionSet.EXPECT().Insert(cas.CachingDirectoryFetcherKey{
			DigestKey:  directoryDigests[10].GetKey(digest.KeyWithoutInstance),
			IsTreeRoot: false,
		})

		directory, err = directoryFetcher.GetDirectory(ctx, directoryDigests[10])
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directoryToInsert, directory)

		// The directory that was evicted should no longer be
		// readable without performing a read.
		baseDirectoryFetcher.EXPECT().GetDirectory(ctx, directoryDigests[3]).Return(nil, status.Error(codes.Internal, "I/O error"))

		_, err = directoryFetcher.GetDirectory(ctx, directoryDigests[3])
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})
}

func TestCachingDirectoryFetcherGetTreeRootDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseDirectoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	evictionSet := mock.NewMockCachingDirectoryFetcherEvictionSet(ctrl)
	directoryFetcher := cas.NewCachingDirectoryFetcher(baseDirectoryFetcher, digest.KeyWithoutInstance, 10, 1000, evictionSet)

	// We assume that the tests of GetDirectory() sufficiently test
	// the caching logic of this type.

	t.Run("Success", func(t *testing.T) {
		// Insert a Tree's root directory into the cache. The
		// key must be made distinct from regular directories
		// using the IsTreeRoot flag, as the digest refers to
		// that of the tree; not the directory.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "72e8cc2780afe06ccaf5353ff29e8bf0", 123151)
		directoryToInsert := &remoteexecution.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target",
				},
			},
		}
		baseDirectoryFetcher.EXPECT().GetTreeRootDirectory(ctx, treeDigest).Return(directoryToInsert, nil)
		evictionSet.EXPECT().Insert(cas.CachingDirectoryFetcherKey{
			DigestKey:  "3-72e8cc2780afe06ccaf5353ff29e8bf0-123151",
			IsTreeRoot: true,
		})

		directory, err := directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directoryToInsert, directory)

		// Request the cached copy of the directory.
		evictionSet.EXPECT().Touch(cas.CachingDirectoryFetcherKey{
			DigestKey:  "3-72e8cc2780afe06ccaf5353ff29e8bf0-123151",
			IsTreeRoot: true,
		})

		directory, err = directoryFetcher.GetTreeRootDirectory(ctx, treeDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directoryToInsert, directory)
	})
}

func TestCachingDirectoryFetcherGetTreeChildDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseDirectoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	evictionSet := mock.NewMockCachingDirectoryFetcherEvictionSet(ctrl)
	directoryFetcher := cas.NewCachingDirectoryFetcher(baseDirectoryFetcher, digest.KeyWithoutInstance, 10, 1000, evictionSet)

	// We assume that the tests of GetDirectory() sufficiently test
	// the caching logic of this type.

	t.Run("Success", func(t *testing.T) {
		// Insert a Tree's child directory into the cache. The
		// key must be based on the digest of the child
		// directory and can be of the same format as that of
		// GetDirectory(), as that one matche the directory's
		// contents.
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "72e8cc2780afe06ccaf5353ff29e8bf0", 123151)
		childDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "63e6ebccf1fae98faba9c59888991621", 72)
		directoryToInsert := &remoteexecution.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "target",
				},
			},
		}
		baseDirectoryFetcher.EXPECT().GetTreeChildDirectory(ctx, treeDigest, childDigest).Return(directoryToInsert, nil)
		evictionSet.EXPECT().Insert(cas.CachingDirectoryFetcherKey{
			DigestKey:  "3-63e6ebccf1fae98faba9c59888991621-72",
			IsTreeRoot: false,
		})

		directory, err := directoryFetcher.GetTreeChildDirectory(ctx, treeDigest, childDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directoryToInsert, directory)

		// Request the cached copy of the directory.
		evictionSet.EXPECT().Touch(cas.CachingDirectoryFetcherKey{
			DigestKey:  "3-63e6ebccf1fae98faba9c59888991621-72",
			IsTreeRoot: false,
		})

		directory, err = directoryFetcher.GetTreeChildDirectory(ctx, treeDigest, childDigest)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, directoryToInsert, directory)
	})
}
