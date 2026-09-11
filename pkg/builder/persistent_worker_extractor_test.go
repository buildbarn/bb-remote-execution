package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestPersistentWorkerExtractor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_MD5)
	emptyDirectoryDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	emptyDirectoryDigestProto := &remoteexecution.Digest{
		Hash:      "d41d8cd98f00b204e9800998ecf8427e",
		SizeBytes: 0,
	}

	t.Run("NoPlatformProperties", func(t *testing.T) {
		// Actions that don't carry a 'persistentWorkerKey'
		// platform property should be executed by spawning a
		// process, meaning no access to storage should be
		// performed at all.
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
		})
		require.NoError(t, err)
		require.Nil(t, persistentWorker)
	})

	t.Run("UnrelatedPlatformProperties", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "container-image", Value: "docker://example.com/image"},
					{Name: "OSFamily", Value: "linux"},
				},
			},
		})
		require.NoError(t, err)
		require.Nil(t, persistentWorker)
	})

	t.Run("EmptyInputRoot", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
		}, persistentWorker)
	})

	t.Run("NestedInputRoot", func(t *testing.T) {
		// Files in the input root need to be reported to the
		// worker process, using paths that are relative to the
		// input root. Digests need to be provided in their
		// hexadecimal notation, encoded as UTF-8.
		rootDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6b9e4b3f5e0a1c05b9b2a4c8a2a09e94", 123)
		srcDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 45)
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), rootDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "BUILD",
						Digest: &remoteexecution.Digest{
							Hash:      "e10adc3949ba59abbe56e057f20f883e",
							SizeBytes: 6,
						},
					},
				},
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "src",
						Digest: &remoteexecution.Digest{
							Hash:      "8b1a9953c4611296a827abf8c47804d7",
							SizeBytes: 45,
						},
					},
				},
				Symlinks: []*remoteexecution.SymlinkNode{
					{
						Name:   "latest",
						Target: "src",
					},
				},
			}, nil)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), srcDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "hello.java",
						Digest: &remoteexecution.Digest{
							Hash:      "5d41402abc4b2a76b9719d911017c592",
							SizeBytes: 5,
						},
					},
					{
						Name: "world.java",
						Digest: &remoteexecution.Digest{
							Hash:      "7d793037a0760186574b0282f2f435e7",
							SizeBytes: 5,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6b9e4b3f5e0a1c05b9b2a4c8a2a09e94",
				SizeBytes: 123,
			},
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "OSFamily", Value: "linux"},
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
			Inputs: []*bazelworker.Input{
				{
					Path:   "BUILD",
					Digest: []byte("e10adc3949ba59abbe56e057f20f883e"),
				},
				{
					Path:   "src/hello.java",
					Digest: []byte("5d41402abc4b2a76b9719d911017c592"),
				},
				{
					Path:   "src/world.java",
					Digest: []byte("7d793037a0760186574b0282f2f435e7"),
				},
			},
		}, persistentWorker)
	})

	t.Run("JSONProtocol", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
					{Name: "persistentWorkerProtocol", Value: "json"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_JSON,
		}, persistentWorker)
	})

	t.Run("ExplicitProtoProtocol", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
					{Name: "persistentWorkerProtocol", Value: "proto"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
		}, persistentWorker)
	})

	t.Run("UnsupportedProtocol", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
					{Name: "persistentWorkerProtocol", Value: "yaml"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Platform property \"persistentWorkerProtocol\" has unsupported value \"yaml\""),
			err,
		)
	})

	t.Run("TooManyInputFiles", func(t *testing.T) {
		// Actions with a large number of input files should be
		// executed as regular processes, as sending the full
		// list of inputs to the runner would be too costly.
		// Falling back is safe, because tools that support
		// running as a persistent worker are also capable of
		// running in one-shot mode.
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "a",
						Digest: &remoteexecution.Digest{
							Hash:      "0cc175b9c0f1b6a831c399e269772661",
							SizeBytes: 1,
						},
					},
					{
						Name: "b",
						Digest: &remoteexecution.Digest{
							Hash:      "92eb5ffee6ae2fec3ad71c777531578f",
							SizeBytes: 1,
						},
					},
					{
						Name: "c",
						Digest: &remoteexecution.Digest{
							Hash:      "4a8a08f09d37b73795649038408b5f33",
							SizeBytes: 1,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 2)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		require.Nil(t, persistentWorker)
	})

	t.Run("ExactlyAtInputFileLimit", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "a",
						Digest: &remoteexecution.Digest{
							Hash:      "0cc175b9c0f1b6a831c399e269772661",
							SizeBytes: 1,
						},
					},
					{
						Name: "b",
						Digest: &remoteexecution.Digest{
							Hash:      "92eb5ffee6ae2fec3ad71c777531578f",
							SizeBytes: 1,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 2)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
			Inputs: []*bazelworker.Input{
				{
					Path:   "a",
					Digest: []byte("0cc175b9c0f1b6a831c399e269772661"),
				},
				{
					Path:   "b",
					Digest: []byte("92eb5ffee6ae2fec3ad71c777531578f"),
				},
			},
		}, persistentWorker)
	})

	t.Run("InvalidInputRootDigest", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "This is not a valid hash",
				SizeBytes: 123,
			},
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Failed to extract digest for input root: Hash has length 24, while 32 characters were expected"),
			err,
		)
	})

	t.Run("StorageFailure", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(nil, status.Error(codes.Internal, "Server on fire"))
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to obtain input directory \".\": Server on fire"),
			err,
		)
	})

	t.Run("NestedStorageFailure", func(t *testing.T) {
		childDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 45)
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "src",
						Digest: &remoteexecution.Digest{
							Hash:      "8b1a9953c4611296a827abf8c47804d7",
							SizeBytes: 45,
						},
					},
				},
			}, nil)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), childDigest).
			Return(nil, status.Error(codes.Unavailable, "Server offline"))
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Failed to obtain input directory \"src\": Server offline"),
			err,
		)
	})

	t.Run("InvalidFilename", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "..",
						Digest: &remoteexecution.Digest{
							Hash:      "0cc175b9c0f1b6a831c399e269772661",
							SizeBytes: 1,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Input directory \".\" contains file with invalid name \"..\""),
			err,
		)
	})

	t.Run("InvalidDirectoryName", func(t *testing.T) {
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "a/b",
						Digest: &remoteexecution.Digest{
							Hash:      "8b1a9953c4611296a827abf8c47804d7",
							SizeBytes: 45,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 100)

		_, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Input directory \".\" contains directory with invalid name \"a/b\""),
			err,
		)
	})

	t.Run("NoInputFileLimit", func(t *testing.T) {
		// A maximum input file count of zero disables the
		// limit entirely.
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), emptyDirectoryDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "a",
						Digest: &remoteexecution.Digest{
							Hash:      "0cc175b9c0f1b6a831c399e269772661",
							SizeBytes: 1,
						},
					},
				},
			}, nil)
		extractor := builder.NewPersistentWorkerExtractor(directoryFetcher, 0)

		persistentWorker, err := extractor.Extract(ctx, digestFunction, &remoteexecution.Action{
			InputRootDigest: emptyDirectoryDigestProto,
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
			Inputs: []*bazelworker.Input{
				{
					Path:   "a",
					Digest: []byte("0cc175b9c0f1b6a831c399e269772661"),
				},
			},
		}, persistentWorker)
	})
}
