package builder_test

import (
	"context"
	"net/url"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	status_pb "google.golang.org/genproto/googleapis/rpc/status"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only when no error is defined in the ExecuteResult and DoNotCache is
// not set, are we allowed to store it in the Action Cache.
func TestCachingBuildExecutorCachedSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			require.True(t, proto.Equal(&remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult))
			return nil
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Message: "Action details (cached result): https://example.com/some/sub/directory/action/freebsd12/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c/11/",
	}, executeResponse))
}

// An explicit Status object with an 'OK' code should also be treated as
// success. The entry should still be stored in the Action Cache.
func TestCachingBuildExecutorCachedSuccessExplicitOK(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status: &status_pb.Status{Message: "This is not an error, because it has code zero"},
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			require.True(t, proto.Equal(&remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult))
			return nil
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status:  &status_pb.Status{Message: "This is not an error, because it has code zero"},
		Message: "Action details (cached result): https://example.com/some/sub/directory/action/freebsd12/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c/11/",
	}, executeResponse))
}

// Test that even when the exit code of the build action is non-zero, we
// still store the result in the Action Cache. Bazel permits this
// nowadays. More details:
// https://github.com/bazelbuild/bazel/issues/7137
func TestCachingBuildExecutorCachedSuccessNonZeroExitCode(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  127,
			StderrRaw: []byte("Compiler error!"),
		},
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			require.True(t, proto.Equal(&remoteexecution.ActionResult{
				ExitCode:  127,
				StderrRaw: []byte("Compiler error!"),
			}, actionResult))
			return nil
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/some/sub/directory",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode:  127,
			StderrRaw: []byte("Compiler error!"),
		},
		Message: "Action details (cached result): https://example.com/some/sub/directory/action/freebsd12/64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c/11/",
	}, executeResponse))
}

// In case we fail to write an entry into the Action Cache, we should
// return the original response, but with the error attached to it.
func TestCachingBuildExecutorCachedStorageFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	actionCache := mock.NewMockBlobAccess(ctrl)
	actionCache.EXPECT().Put(
		ctx,
		digest.MustNewDigest("freebsd12", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
		gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 10000)
			require.NoError(t, err)
			require.True(t, proto.Equal(&remoteexecution.ActionResult{
				StdoutRaw: []byte("Hello, world!"),
			}, actionResult))
			return status.Error(codes.Internal, "Network problems")
		})
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "https",
		Host:   "example.com",
		Path:   "/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Status: status.New(codes.Internal, "Failed to store cached action result: Network problems").Proto(),
	}, executeResponse))
}

// When the DoNotCache flag is set, we should not store results in the
// Action Cache.
func TestCachingBuildExecutorUncachedDoNotCache(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutUncachedActionResult(
		ctx,
		&cas_proto.UncachedActionResult{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			},
			ExecuteResponse: &remoteexecution.ExecuteResponse{
				Result: &remoteexecution.ActionResult{
					StdoutRaw: []byte("Hello, world!"),
				},
			},
		},
		gomock.Any()).Return(
		digest.MustNewDigest("freebsd12", "1204703084039248092148032948092148032948034924802194802138213222", 582),
		nil)
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Hello, world!"),
		},
		Message: "Action details (uncached result): http://example.com/some/sub/directory/uncached_action_result/freebsd12/1204703084039248092148032948092148032948034924802194802138213222/582/",
	}, executeResponse))
}

// We should also not store results in the Action Cache when an error is
// part of the ExecuteResponse.
func TestCachingBuildExecutorUncachedError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutUncachedActionResult(
		ctx,
		&cas_proto.UncachedActionResult{
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
		},
		gomock.Any()).Return(
		digest.MustNewDigest("freebsd12", "1204703084039248092148032948092148032948034924802194802138213222", 582),
		nil)
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status:  status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
		Message: "Action details (uncached result): http://example.com/some/sub/directory/uncached_action_result/freebsd12/1204703084039248092148032948092148032948034924802194802138213222/582/",
	}, executeResponse))
}

// An error generated by uploading the UncachedActionResult should not
// overwrite the error that is already part of the ExecuteResponse.
func TestCachingBuildExecutorUncachedStorageFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
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
	var metadata chan<- *remoteworker.CurrentState_Executing = make(chan *remoteworker.CurrentState_Executing, 10)
	baseBuildExecutor.EXPECT().Execute(ctx, filePool, "freebsd12", request, metadata).Return(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	})
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutUncachedActionResult(
		ctx,
		&cas_proto.UncachedActionResult{
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
		},
		gomock.Any()).Return(
		digest.MustNewDigest("freebsd12", "1204703084039248092148032948092148032948034924802194802138213222", 582),
		status.Error(codes.Internal, "Cannot store uncached action result"))
	actionCache := mock.NewMockBlobAccess(ctrl)
	cachingBuildExecutor := builder.NewCachingBuildExecutor(baseBuildExecutor, contentAddressableStorage, actionCache, &url.URL{
		Scheme: "http",
		Host:   "example.com",
		Path:   "/some/sub/directory/",
	})

	executeResponse := cachingBuildExecutor.Execute(ctx, filePool, "freebsd12", request, metadata)
	require.True(t, proto.Equal(&remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutRaw: []byte("Compiling..."),
		},
		Status: status.New(codes.DeadlineExceeded, "Build took more than ten seconds").Proto(),
	}, executeResponse))
}
