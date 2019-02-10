package builder_test

import (
	"context"
	"net/url"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCachingBuildExecutorBadActionDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockActionCache(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: No digest provided").Proto(),
	}, executeResponse)
	require.False(t, mayBeCached)
}

func TestCachingBuildExecutorNoResult(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	baseBuildExecutor.EXPECT().Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	}).Return(&remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Hard disk on fire").Proto(),
	}, false)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockActionCache(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Status:  status.New(codes.Internal, "Hard disk on fire").Proto(),
		Message: "Action details (no result): https://example.com/action/freebsd12/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c/11/",
	}, executeResponse)
	require.False(t, mayBeCached)
}

func TestCachingBuildExecutorCachedSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	baseBuildExecutor.EXPECT().Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	}).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	}, true)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockActionCache(ctrl)
	actionCache.EXPECT().PutActionResult(
		ctx,
		util.MustNewDigest("freebsd12", &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}),
		&remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		}).Return(nil)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Message: "Action details (cached result): https://example.com/action/freebsd12/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c/11/",
	}, executeResponse)
	require.True(t, mayBeCached)
}

func TestCachingBuildExecutorCachedFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	baseBuildExecutor.EXPECT().Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	}).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	}, true)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockActionCache(ctrl)
	actionCache.EXPECT().PutActionResult(
		ctx,
		util.MustNewDigest("freebsd12", &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}),
		&remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		}).Return(status.Error(codes.Internal, "Network problems"))
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Failed to store cached action result: Network problems").Proto(),
	}, executeResponse)
	require.False(t, mayBeCached)
}

func TestCachingBuildExecutorUncachedSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	baseBuildExecutor.EXPECT().Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	}).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  1,
			StderrRaw: []byte("Compilation failed"),
		},
	}, false)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutUncachedActionResult(
		ctx,
		&cas_proto.UncachedActionResult{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			},
			ActionResult: &remoteexecution.ActionResult{
				ExitCode:  1,
				StderrRaw: []byte("Compilation failed"),
			},
		},
		gomock.Any()).Return(util.MustNewDigest("freebsd12", &remoteexecution.Digest{
		Hash:      "1204703084039248092148032948092148032948034924802194802138213222",
		SizeBytes: 582,
	}), nil)
	actionCache := mock.NewMockActionCache(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  1,
			StderrRaw: []byte("Compilation failed"),
		},
		Message: "Action details (uncached result): https://example.com/uncached_action_result/freebsd12/1204703084039248092148032948092148032948034924802194802138213222/582/",
	}, executeResponse)
	require.False(t, mayBeCached)
}

func TestCachingBuildExecutorUncachedFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	baseBuildExecutor.EXPECT().Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	}).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  1,
			StderrRaw: []byte("Compilation failed"),
		},
	}, false)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutUncachedActionResult(
		ctx,
		&cas_proto.UncachedActionResult{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			},
			ActionResult: &remoteexecution.ActionResult{
				ExitCode:  1,
				StderrRaw: []byte("Compilation failed"),
			},
		},
		gomock.Any()).Return(nil, status.Error(codes.Internal, "Network problems"))
	actionCache := mock.NewMockActionCache(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
	})

	executeResponse, mayBeCached := cachingBuildExecutor.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "freebsd12",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		},
	})
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Internal, "Failed to store uncached action result: Network problems").Proto(),
	}, executeResponse)
	require.False(t, mayBeCached)
}
