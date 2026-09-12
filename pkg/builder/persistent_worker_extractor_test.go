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

	t.Run("ToolInputs", func(t *testing.T) {
		// Files that Bazel marked with the 'bazel_tool_input'
		// node property need to be reported separately, so that
		// the runner can give them a home that outlives the
		// build action. A directory that holds nothing but tool
		// inputs is collapsed into a single path, so that the
		// runner does not need to descend into it.
		rootDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6b9e4b3f5e0a1c05b9b2a4c8a2a09e94", 123)
		jdkDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 45)
		binDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "c4ca4238a0b923820dcc509a6f75849b", 12)
		mixedDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "c81e728d9d4c2f636f067f89cc14862c", 34)
		toolInput := &remoteexecution.NodeProperties{
			Properties: []*remoteexecution.NodeProperty{
				{Name: "bazel_tool_input"},
			},
		}
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), rootDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:   "Hello.java",
						Digest: &remoteexecution.Digest{Hash: "5d41402abc4b2a76b9719d911017c592", SizeBytes: 5},
					},
				},
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name:   "jdk",
						Digest: &remoteexecution.Digest{Hash: "8b1a9953c4611296a827abf8c47804d7", SizeBytes: 45},
					},
					{
						Name:   "mixed",
						Digest: &remoteexecution.Digest{Hash: "c81e728d9d4c2f636f067f89cc14862c", SizeBytes: 34},
					},
				},
			}, nil)
		// Every file underneath "jdk" belongs to the tool, so
		// the whole directory collapses into one path.
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), jdkDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:           "release",
						Digest:         &remoteexecution.Digest{Hash: "7d793037a0760186574b0282f2f435e7", SizeBytes: 5},
						NodeProperties: toolInput,
					},
				},
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name:   "bin",
						Digest: &remoteexecution.Digest{Hash: "c4ca4238a0b923820dcc509a6f75849b", SizeBytes: 12},
					},
				},
			}, nil)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), binDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:           "java",
						Digest:         &remoteexecution.Digest{Hash: "8277e0910d750195b448797616e091ad", SizeBytes: 7},
						NodeProperties: toolInput,
					},
				},
			}, nil)
		// "mixed" holds a tool input next to a regular input,
		// so only the tool input itself is reported.
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), mixedDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:           "JavaBuilder.jar",
						Digest:         &remoteexecution.Digest{Hash: "e1671797c52e15f763380b45e841ec32", SizeBytes: 9},
						NodeProperties: toolInput,
					},
					{
						Name:   "sources.txt",
						Digest: &remoteexecution.Digest{Hash: "1679091c5a880faf6fb5e6087eb1b2dc", SizeBytes: 6},
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
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
			Inputs: []*bazelworker.Input{
				{Path: "Hello.java", Digest: []byte("5d41402abc4b2a76b9719d911017c592")},
				{Path: "jdk/release", Digest: []byte("7d793037a0760186574b0282f2f435e7")},
				{Path: "jdk/bin/java", Digest: []byte("8277e0910d750195b448797616e091ad")},
				{Path: "mixed/JavaBuilder.jar", Digest: []byte("e1671797c52e15f763380b45e841ec32")},
				{Path: "mixed/sources.txt", Digest: []byte("1679091c5a880faf6fb5e6087eb1b2dc")},
			},
			ToolInputPaths: []string{
				"jdk",
				"mixed/JavaBuilder.jar",
			},
		}, persistentWorker)
	})

	t.Run("ToolInputsNeverCollapseInputRoot", func(t *testing.T) {
		// Even when every file in the input root belongs to the
		// tool, the input root itself must not be reported as a
		// tool input path. It is the directory that the runner
		// populates with symbolic links.
		rootDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6b9e4b3f5e0a1c05b9b2a4c8a2a09e94", 123)
		directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
		directoryFetcher.EXPECT().GetDirectory(gomock.Any(), rootDigest).
			Return(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name:   "tool",
						Digest: &remoteexecution.Digest{Hash: "5d41402abc4b2a76b9719d911017c592", SizeBytes: 5},
						NodeProperties: &remoteexecution.NodeProperties{
							Properties: []*remoteexecution.NodeProperty{
								{Name: "bazel_tool_input"},
							},
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
					{Name: "persistentWorkerKey", Value: "b0a6c1"},
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &runner_pb.PersistentWorker{
			Key:      "b0a6c1",
			Protocol: runner_pb.PersistentWorker_PROTO,
			Inputs: []*bazelworker.Input{
				{Path: "tool", Digest: []byte("5d41402abc4b2a76b9719d911017c592")},
			},
			ToolInputPaths: []string{"tool"},
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
