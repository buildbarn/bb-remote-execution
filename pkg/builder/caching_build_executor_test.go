package builder_test

import (
	"context"
	"net/url"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	cas_proto "github.com/buildbarn/bb-remote-execution/pkg/proto/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	status_pb "google.golang.org/genproto/googleapis/rpc/status"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only when no error is defined in the ExecuteResult and DoNotCache is
// not set, are we allowed to store it in the Action Cache.
func TestCachingBuildExecutorCachedSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult)
			return nil
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Message: "Action details (cached result): https://example.com/some/sub/directory/freebsd12/blobs/sha256/action/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c-11/",
	}, executeResponse)
}

// An explicit Status object with an 'OK' code should also be treated as
// success. The entry should still be stored in the Action Cache.
func TestCachingBuildExecutorCachedSuccessExplicitOK(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status: &status_pb.Status{Message: "This is not an error, because it has code zero"},
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult)
			return nil
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status:  &status_pb.Status{Message: "This is not an error, because it has code zero"},
		Message: "Action details (cached result): https://example.com/some/sub/directory/freebsd12/blobs/sha256/action/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c-11/",
	}, executeResponse)
}

// When the exit code of the build action is non-zero, we may store the
// result in the Action Cache. Bazel permits this nowadays. More details:
// https://github.com/bazelbuild/bazel/issues/7137
//
// This implementation decides to store an entry in the Content
// Addressable Storage regardless, as that allows us to obtain stable
// URLs to build failures.
func TestCachingBuildExecutorCachedSuccessNonZeroExitCode(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  127,
			StderrRaw: []byte("Compiler error!"),
		},
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "bb1107706f3aa379d68aa61062f56d99d24a667ec18d5756fb6df1ba9baa1fdc", 93),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			historicalExecuteResponse, err := b.ToProto(&cas_proto.HistoricalExecuteResponse{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &cas_proto.HistoricalExecuteResponse{
				ActionDigest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
				ExecuteResponse: &remoteexecution.ExecuteResponse{
					Result: &remoteexecution.ActionResult{
						ExitCode:  127,
						StderrRaw: []byte("Compiler error!"),
					},
				},
			}, historicalExecuteResponse)
			return nil
		})
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  127,
			StderrRaw: []byte("Compiler error!"),
		},
		Message: "Action details (uncached result): https://example.com/some/sub/directory/freebsd12/blobs/sha256/historical_execute_response/bb1107706f3aa379d68aa61062f56d99d24a667ec18d5756fb6df1ba9baa1fdc-93/",
	}, executeResponse)
}

// In case we fail to write an entry into the Action Cache, we should
// return the original response, but with the error attached to it.
func TestCachingBuildExecutorCachedStorageFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult)
			return status.Error(codes.Internal, "Network problems")
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status: status.New(codes.Internal, "Failed to store cached action result: Network problems").Proto(),
	}, executeResponse)
}

// When the DoNotCache flag is set, we should not store results in the
// Action Cache.
func TestCachingBuildExecutorUncachedDoNotCache(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: true}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "5ed2d5720b99f5575542bb4f89e84b5e00e34ab652292974fdb814ab7dc3c92e", 89),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			historicalExecuteResponse, err := b.ToProto(&cas_proto.HistoricalExecuteResponse{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &cas_proto.HistoricalExecuteResponse{
				ActionDigest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
				ExecuteResponse: &remoteexecution.ExecuteResponse{
					Result: &remoteexecution.ActionResult{
						StdoutRaw: []byte("Hello, world!"),
					},
				},
			}, historicalExecuteResponse)
			return nil
		})
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Message: "Action details (uncached result): http://example.com/some/sub/directory/freebsd12/blobs/sha256/historical_execute_response/5ed2d5720b99f5575542bb4f89e84b5e00e34ab652292974fdb814ab7dc3c92e-89/",
	}, executeResponse)
}

// We should also not store results in the Action Cache when an error is
// part of the ExecuteResponse.
func TestCachingBuildExecutorUncachedError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "a6e4f00dd21540b0b653dcd195b3d54ea4c0b3ca679cf6a69eb7b0dbd378c2cc", 126),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			historicalExecuteResponse, err := b.ToProto(&cas_proto.HistoricalExecuteResponse{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &cas_proto.HistoricalExecuteResponse{
				ActionDigest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
				ExecuteResponse: &remoteexecution.ExecuteResponse{
					Result: &remoteexecution.ActionResult{
						StdoutRaw: []byte("Compiling..."),
					},
					Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
				},
			}, historicalExecuteResponse)
			return nil
		})
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status:  status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
		Message: "Action details (uncached result): http://example.com/some/sub/directory/freebsd12/blobs/sha256/historical_execute_response/a6e4f00dd21540b0b653dcd195b3d54ea4c0b3ca679cf6a69eb7b0dbd378c2cc-126/",
	}, executeResponse)
}

// An error generated by uploading the HistoricalExecuteResponse should not
// overwrite the error that is already part of the ExecuteResponse.
func TestCachingBuildExecutorUncachedStorageFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	baseBuildExecutor := mock.NewMockBuildExecutor(ctrl)
	actionDigest := &remoteexecution.Digest{
		Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
		SizeBytes: 11,
	}
	action := &remoteexecution.Action{DoNotCache: false}
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: actionDigest,
		Action:       action,
	}
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	digestFunction := digest.MustNewFunction("freebsd12", remoteexecution.DigestFunction_SHA256)
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, monitor, digestFunction, request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	})
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", remoteexecution.DigestFunction_SHA256, "a6e4f00dd21540b0b653dcd195b3d54ea4c0b3ca679cf6a69eb7b0dbd378c2cc", 126),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			historicalExecuteResponse, err := b.ToProto(&cas_proto.HistoricalExecuteResponse{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &cas_proto.HistoricalExecuteResponse{
				ActionDigest: &remoteexecution.Digest{
					Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
					SizeBytes: 11,
				},
				ExecuteResponse: &remoteexecution.ExecuteResponse{
					Result: &remoteexecution.ActionResult{
						StdoutRaw: []byte("Compiling..."),
					},
					Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
				},
			}, historicalExecuteResponse)
			return status.Error(codes.Internal, "Cannot store historical execute response")
		})
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, monitor, digestFunction, request, metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	}, executeResponse)
}
