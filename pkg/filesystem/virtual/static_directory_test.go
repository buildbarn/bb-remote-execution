package virtual_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestStaticDirectoryVirtualLookup(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	child := mock.NewMockVirtualDirectory(ctrl)
	d := virtual.NewStaticDirectory(map[path.Component]virtual.DirectoryChild{
		path.MustNewComponent("child"): virtual.DirectoryChild{}.FromDirectory(child),
	})

	t.Run("NotFound", func(t *testing.T) {
		var out virtual.Attributes
		_, s := d.VirtualLookup(ctx, path.MustNewComponent("nonexistent"), 0, &out)
		require.Equal(t, virtual.StatusErrNoEnt, s)
	})

	t.Run("Success", func(t *testing.T) {
		child.EXPECT().VirtualGetAttributes(
			ctx,
			virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
			out.SetInodeNumber(456)
		})

		var out virtual.Attributes
		actualChild, s := d.VirtualLookup(ctx, path.MustNewComponent("child"), virtual.AttributesMaskInodeNumber, &out)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, virtual.DirectoryChild{}.FromDirectory(child), actualChild)
		require.Equal(t, *(&virtual.Attributes{}).SetInodeNumber(456), out)
	})
}

func TestStaticDirectoryVirtualOpenChild(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	child := mock.NewMockVirtualDirectory(ctrl)
	d := virtual.NewStaticDirectory(map[path.Component]virtual.DirectoryChild{
		path.MustNewComponent("child"): virtual.DirectoryChild{}.FromDirectory(child),
	})

	t.Run("NotFound", func(t *testing.T) {
		// Child does not exist, and we're not instructed to
		// create anything.
		var out virtual.Attributes
		_, _, _, s := d.VirtualOpenChild(
			ctx,
			path.MustNewComponent("nonexistent"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, virtual.StatusErrNoEnt, s)
	})

	t.Run("ReadOnlyFileSystem", func(t *testing.T) {
		// Child does not exist, and we're don't support
		// creating anything.
		var out virtual.Attributes
		_, _, _, s := d.VirtualOpenChild(
			ctx,
			path.MustNewComponent("nonexistent"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, virtual.StatusErrROFS, s)
	})

	t.Run("Exists", func(t *testing.T) {
		// A directory already exists under this name, so we
		// can't create a file.
		var out virtual.Attributes
		_, _, _, s := d.VirtualOpenChild(
			ctx,
			path.MustNewComponent("child"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
			nil,
			virtual.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, virtual.StatusErrExist, s)
	})

	t.Run("IsDirectory", func(t *testing.T) {
		// A directory already exists under this name, so we
		// can't open it as a file.
		var out virtual.Attributes
		_, _, _, s := d.VirtualOpenChild(
			ctx,
			path.MustNewComponent("child"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, virtual.StatusErrIsDir, s)
	})
}

func TestStaticDirectoryVirtualReadDir(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	childA := mock.NewMockVirtualDirectory(ctrl)
	childB := mock.NewMockVirtualDirectory(ctrl)
	d := virtual.NewStaticDirectory(map[path.Component]virtual.DirectoryChild{
		path.MustNewComponent("a"): virtual.DirectoryChild{}.FromDirectory(childA),
		path.MustNewComponent("b"): virtual.DirectoryChild{}.FromDirectory(childB),
	})

	t.Run("FromStart", func(t *testing.T) {
		// Read the directory in its entirety.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childA.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetInodeNumber(123)
			})
		reporter.EXPECT().ReportEntry(
			uint64(1),
			path.MustNewComponent("a"),
			virtual.DirectoryChild{}.FromDirectory(childA),
			(&virtual.Attributes{}).SetInodeNumber(123),
		).Return(true)
		childB.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetInodeNumber(456)
			})
		reporter.EXPECT().ReportEntry(
			uint64(2),
			path.MustNewComponent("b"),
			virtual.DirectoryChild{}.FromDirectory(childB),
			(&virtual.Attributes{}).SetInodeNumber(456),
		).Return(true)

		require.Equal(
			t,
			virtual.StatusOK,
			d.VirtualReadDir(ctx, 0, virtual.AttributesMaskInodeNumber, reporter))
	})

	t.Run("NoSpace", func(t *testing.T) {
		// Not even enough space to store the first entry.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childA.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetInodeNumber(123)
			})
		reporter.EXPECT().ReportEntry(
			uint64(1),
			path.MustNewComponent("a"),
			virtual.DirectoryChild{}.FromDirectory(childA),
			(&virtual.Attributes{}).SetInodeNumber(123),
		).Return(false)

		require.Equal(
			t,
			virtual.StatusOK,
			d.VirtualReadDir(ctx, 0, virtual.AttributesMaskInodeNumber, reporter))
	})

	t.Run("Partial", func(t *testing.T) {
		// Only read the last entry.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childB.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(ctx context.Context, requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetInodeNumber(456)
			})
		reporter.EXPECT().ReportEntry(
			uint64(2),
			path.MustNewComponent("b"),
			virtual.DirectoryChild{}.FromDirectory(childB),
			(&virtual.Attributes{}).SetInodeNumber(456),
		).Return(true)

		require.Equal(
			t,
			virtual.StatusOK,
			d.VirtualReadDir(ctx, 1, virtual.AttributesMaskInodeNumber, reporter))
	})

	t.Run("AtEOF", func(t *testing.T) {
		// Reading at the end-of-file should yield no entries.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			virtual.StatusOK,
			d.VirtualReadDir(ctx, 2, virtual.AttributesMaskInodeNumber, reporter))
	})

	t.Run("BeyondEOF", func(t *testing.T) {
		// Reading past the end-of-file should not cause incorrect
		// behaviour.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			virtual.StatusOK,
			d.VirtualReadDir(ctx, 3, virtual.AttributesMaskInodeNumber, reporter))
	})
}
