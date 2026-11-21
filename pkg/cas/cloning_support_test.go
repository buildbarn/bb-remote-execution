package cas_test

import (
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestCheckCloningSupported(t *testing.T) {
	ctrl := gomock.NewController(t)
	probe := path.MustNewComponent(".clonefile_probe")

	t.Run("Unsupported", func(t *testing.T) {
		// A volume that supports hardlinks but not clones must be
		// reported at startup, naming both directories, instead of
		// failing every build action the worker would accept.
		cacheDirectory := mock.NewMockDirectory(ctrl)
		buildDirectory := mock.NewMockDirectory(ctrl)
		probeFile := mock.NewMockFileWriter(ctrl)
		gomock.InOrder(
			buildDirectory.EXPECT().Remove(probe).Return(syscall.ENOENT),
			cacheDirectory.EXPECT().OpenWrite(probe, filesystem.CreateExcl(0o444)).Return(probeFile, nil),
			probeFile.EXPECT().Close(),
			cacheDirectory.EXPECT().Clonefile(probe, buildDirectory, probe).Return(syscall.ENOTSUP),
			cacheDirectory.EXPECT().Remove(probe),
		)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.FailedPrecondition, "Cannot clone files from cache directory \"/cache\" into build directory \"/build\": "+syscall.ENOTSUP.Error()),
			cas.CheckCloningSupported(cacheDirectory, "/cache", buildDirectory, "/build"),
		)
	})

	t.Run("StaleProbe", func(t *testing.T) {
		// A probe left in the build directory by a worker that
		// crashed mid-probe must not make the clone fail with EEXIST
		// on every later startup.
		cacheDirectory := mock.NewMockDirectory(ctrl)
		buildDirectory := mock.NewMockDirectory(ctrl)
		probeFile := mock.NewMockFileWriter(ctrl)
		gomock.InOrder(
			buildDirectory.EXPECT().Remove(probe),
			cacheDirectory.EXPECT().OpenWrite(probe, filesystem.CreateExcl(0o444)).Return(probeFile, nil),
			probeFile.EXPECT().Close(),
			cacheDirectory.EXPECT().Clonefile(probe, buildDirectory, probe),
			buildDirectory.EXPECT().Remove(probe),
			cacheDirectory.EXPECT().Remove(probe),
		)

		require.NoError(t, cas.CheckCloningSupported(cacheDirectory, "/cache", buildDirectory, "/build"))
	})
}
