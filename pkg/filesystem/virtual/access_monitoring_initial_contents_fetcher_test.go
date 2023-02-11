package virtual_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAccessMonitoringInitialContentsFetcher(t *testing.T) {
	ctrl := gomock.NewController(t)

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
		rootReadDirectoryMonitor := mock.NewMockReadDirectoryMonitor(ctrl)
		rootUnreadDirectoryMonitor.EXPECT().ReadDirectory().Return(rootReadDirectoryMonitor)
		baseFileReadMonitorFactory := mock.NewMockFileReadMonitorFactory(ctrl)
		baseInitialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
			Return(nil, status.Error(codes.Internal, "Network error"))

		_, err := initialContentsFetcher.FetchContents(baseFileReadMonitorFactory.Call)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Network error"), err)
	})

	t.Run("FetchContentsSucceeded", func(t *testing.T) {
		// Reading the directory's contents should report it as being
		// read. It should return children that are wrapped as well.
		baseChildInitialContentsFetcher := mock.NewMockInitialContentsFetcher(ctrl)
		baseChildFile := mock.NewMockNativeLeaf(ctrl)
		baseChildFileReadMonitor := mock.NewMockFileReadMonitor(ctrl)
		baseFileReadMonitorFactory := mock.NewMockFileReadMonitorFactory(ctrl)
		baseFileReadMonitorFactory.EXPECT().Call(path.MustNewComponent("file")).Return(baseChildFileReadMonitor.Call)
		var childFileReadMonitor virtual.FileReadMonitor
		baseInitialContentsFetcher.EXPECT().FetchContents(gomock.Any()).
			DoAndReturn(func(fileReadMonitorFactory virtual.FileReadMonitorFactory) (map[path.Component]virtual.InitialNode, error) {
				childFileReadMonitor = fileReadMonitorFactory(path.MustNewComponent("file"))
				return map[path.Component]virtual.InitialNode{
					path.MustNewComponent("dir"):  virtual.InitialNode{}.FromDirectory(baseChildInitialContentsFetcher),
					path.MustNewComponent("file"): virtual.InitialNode{}.FromLeaf(baseChildFile),
				}, nil
			})
		rootReadDirectoryMonitor := mock.NewMockReadDirectoryMonitor(ctrl)
		rootUnreadDirectoryMonitor.EXPECT().ReadDirectory().Return(rootReadDirectoryMonitor)
		childUnreadDirectoryMonitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
		rootReadDirectoryMonitor.EXPECT().ResolvedDirectory(path.MustNewComponent("dir")).Return(childUnreadDirectoryMonitor)

		rootContents, err := initialContentsFetcher.FetchContents(baseFileReadMonitorFactory.Call)
		require.NoError(t, err)
		require.Len(t, rootContents, 2)

		t.Run("ChildDirectory", func(t *testing.T) {
			childInitialContentsFetcher, _ := rootContents[path.MustNewComponent("dir")].GetPair()

			t.Run("FetchContentsSucceeded", func(t *testing.T) {
				baseChildInitialContentsFetcher.EXPECT().FetchContents(gomock.Any()).Return(map[path.Component]virtual.InitialNode{}, nil)
				childReadDirectoryMonitor := mock.NewMockReadDirectoryMonitor(ctrl)
				childUnreadDirectoryMonitor.EXPECT().ReadDirectory().Return(childReadDirectoryMonitor)
				baseChildFileReadMonitorFactory := mock.NewMockFileReadMonitorFactory(ctrl)

				childContents, err := childInitialContentsFetcher.FetchContents(baseChildFileReadMonitorFactory.Call)
				require.NoError(t, err)
				require.Empty(t, childContents)
			})
		})

		t.Run("ChildFile", func(t *testing.T) {
			_, childFile := rootContents[path.MustNewComponent("file")].GetPair()
			require.Equal(t, baseChildFile, childFile)

			// If a notification is sent that the file's
			// contents have been read, it should be
			// duplicated both to the base file read
			// monitor, and the read directory monitor.
			baseChildFileReadMonitor.EXPECT().Call()
			rootReadDirectoryMonitor.EXPECT().ReadFile(path.MustNewComponent("file"))

			childFileReadMonitor()
		})
	})
}
