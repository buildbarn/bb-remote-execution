package virtual_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAccessMonitoringInitialContentsFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseInitialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
	rootUnreadDirectoryMonitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	initialContentsFetcher := virtual.NewAccessMonitoringInitialContentsFetcher(
		baseInitialContentsFetcher,
		rootUnreadDirectoryMonitor)

	t.Run("FetchContentsFailed", func(t *testing.T) {
		// If fetching the initial contents of a directory
		// fails, we should not mark the directory as being
		// read. The reason being that it may succeed later on.
		// We can't call into ReadDirectory() multiple times.
		baseInitialContentsFetcher.EXPECT().FetchContents().
			Return(nil, status.Error(codes.Internal, "Network error"))

		_, err := initialContentsFetcher.FetchContents()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Network error"), err)
	})

	t.Run("FetchContentsSucceeded", func(t *testing.T) {
		// Reading the directory's contents should report it as being
		// read. It should return children that are wrapped as well.
		baseChildInitialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		baseFile := mock.NewMockNativeLeaf(ctrl)
		baseInitialContentsFetcher.EXPECT().FetchContents().Return(map[path.Component]virtual.InitialNode{
			path.MustNewComponent("dir"):  virtual.InitialNode{}.FromDirectory(baseChildInitialContentsFetcher),
			path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(baseFile),
		}, nil)
		rootReadDirectoryMonitor := mock.NewMockReadDirectoryMonitor(ctrl)
		rootUnreadDirectoryMonitor.EXPECT().ReadDirectory().Return(rootReadDirectoryMonitor)
		childUnreadDirectoryMonitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
		rootReadDirectoryMonitor.EXPECT().ResolvedDirectory(path.MustNewComponent("dir")).Return(childUnreadDirectoryMonitor)

		rootContents, err := initialContentsFetcher.FetchContents()
		require.NoError(t, err)
		require.Len(t, rootContents, 2)

		t.Run("ChildDirectory", func(t *testing.T) {
			childInitialContentsFetcher, _ := rootContents[path.MustNewComponent("dir")].GetPair()

			t.Run("FetchContentsSucceeded", func(t *testing.T) {
				baseChildInitialContentsFetcher.EXPECT().FetchContents().Return(map[path.Component]virtual.InitialNode{}, nil)
				childReadDirectoryMonitor := mock.NewMockReadDirectoryMonitor(ctrl)
				childUnreadDirectoryMonitor.EXPECT().ReadDirectory().Return(childReadDirectoryMonitor)

				childContents, err := childInitialContentsFetcher.FetchContents()
				require.NoError(t, err)
				require.Empty(t, childContents)
			})
		})

		t.Run("ChildFile", func(t *testing.T) {
			_, childFile := rootContents[path.MustNewComponent("file")].GetPair()

			t.Run("UnrelatedOperation", func(t *testing.T) {
				// Calling operations against the file that
				// don't depend on the file's contents (e.g.,
				// VirtualGetAttributes()) should not cause the
				// file to be reported as read.
				baseFile.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileType, gomock.Any()).DoAndReturn(
					func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
						out.SetFileType(filesystem.FileTypeRegularFile)
					})

				var out virtual.Attributes
				childFile.VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, &out)
				require.Equal(t, (&virtual.Attributes{}).SetFileType(filesystem.FileTypeRegularFile), &out)
			})

			t.Run("Read", func(t *testing.T) {
				// Reading the file's contents should cause it
				// to be reported as being read. This should
				// only happen just once.
				rootReadDirectoryMonitor.EXPECT().ReadFile(path.MustNewComponent("file"))
				baseFile.EXPECT().VirtualRead(gomock.Len(5), uint64(0)).
					DoAndReturn(func(buf []byte, off uint64) (int, bool, virtual.Status) {
						copy(buf, "Hello")
						return 5, false, virtual.StatusOK
					}).
					Times(10)

				for i := 0; i < 10; i++ {
					var buf [5]byte
					n, eof, s := childFile.VirtualRead(buf[:], 0)
					require.Equal(t, 5, n)
					require.False(t, eof)
					require.Equal(t, virtual.StatusOK, s)
				}
			})
		})
	})
}
