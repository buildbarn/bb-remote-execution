package builder_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	worker_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
)

func persistentWorkerTestFile(contents string, tool bool) *remoteexecution.FileNode {
	hash := sha256.Sum256([]byte(contents))
	file := &remoteexecution.FileNode{
		Digest: &remoteexecution.Digest{Hash: hex.EncodeToString(hash[:]), SizeBytes: int64(len(contents))},
	}
	if tool {
		file.NodeProperties = &remoteexecution.NodeProperties{Properties: []*remoteexecution.NodeProperty{{Name: "bazel_tool_input"}}}
		file.IsExecutable = true
	}
	return file
}

func persistentWorkerTestCommand() (*remoteexecution.Action, *remoteexecution.Command, map[string]*remoteexecution.FileNode) {
	return &remoteexecution.Action{Platform: &remoteexecution.Platform{Properties: []*remoteexecution.Platform_Property{
			{Name: "persistentWorkerKey", Value: "tool-key"},
			{Name: "persistentWorkerProtocol", Value: "proto"},
			{Name: "OSFamily", Value: "Linux"},
		}}}, &remoteexecution.Command{
			Arguments: []string{"compiler", "--startup", "@args"},
			EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
				{Name: "PATH", Value: "/bin"}, {Name: "LANG", Value: "C"},
			},
		}, map[string]*remoteexecution.FileNode{
			"compiler": persistentWorkerTestFile("compiler", true),
			"args":     persistentWorkerTestFile("source\noutput\n", false),
			"source":   persistentWorkerTestFile("source", false),
		}
}

func TestPersistentWorkerCommandValidation(t *testing.T) {
	for _, test := range []struct {
		name     string
		modify   func(*remoteexecution.Action, *remoteexecution.Command, map[string]*remoteexecution.FileNode)
		eligible bool
		invalid  bool
	}{
		{name: "ValidPlatform", eligible: true},
		{name: "NilPlatform", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			action.Platform = nil
		}},
		{name: "NoMarkedTools", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["compiler"].NodeProperties = nil
		}},
		{name: "MissingArguments", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.Arguments = nil
		}},
		{name: "MissingFlagFile", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.Arguments = []string{"compiler", "source"}
		}},
		{name: "DuplicateEnvironment", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.EnvironmentVariables = append(command.EnvironmentVariables, command.EnvironmentVariables[0])
		}},
		{name: "InvalidDigest", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["source"].Digest.Hash = "invalid"
		}},
		{name: "InvalidInputPath", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["../source"] = inputs["source"]
		}},
		{name: "InvalidWorkingDirectory", invalid: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.WorkingDirectory = "../outside"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			action, command, inputs := persistentWorkerTestCommand()
			if test.modify != nil {
				test.modify(action, command, inputs)
			}
			prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
			if test.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if test.eligible {
				require.NotNil(t, prepared)
			} else {
				require.Nil(t, prepared)
			}
		})
	}
}

func TestPersistentWorkerCommandStartupArguments(t *testing.T) {
	for _, arguments := range [][]string{
		{"compiler", "--startup", "@args"},
		{"java", "-Xfoo", "-jar", "x.jar", "--tool_arg", "@args"},
		{"java", "-Xfoo", "-jar", "x.jar", "@args", "--tool_arg"},
	} {
		t.Run(strings.Join(arguments, " "), func(t *testing.T) {
			action, command, inputs := persistentWorkerTestCommand()
			command.Arguments = slices.Clone(arguments)
			prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
			require.NoError(t, err)
			expected := []string{"compiler", "--startup", "--persistent_worker"}
			if arguments[0] == "java" {
				expected = []string{"java", "-Xfoo", "-jar", "x.jar", "--tool_arg", "--persistent_worker"}
			}
			require.Equal(t, expected, prepared.Arguments)
			require.Equal(t, arguments, command.Arguments)
		})
	}
}

func TestPersistentWorkerCommandCompatibility(t *testing.T) {
	for _, test := range []struct {
		name   string
		modify func(*remoteexecution.Action, *remoteexecution.Command, map[string]*remoteexecution.FileNode)
		same   bool
	}{
		{name: "Same", same: true},
		{name: "Reordered", same: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			slices.Reverse(action.Platform.Properties)
			slices.Reverse(command.EnvironmentVariables)
		}},
		{name: "SourceAndOutput", same: true, modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["source"] = persistentWorkerTestFile("different source", false)
			inputs["other-args"] = persistentWorkerTestFile("other arguments", false)
			command.Arguments[2] = "@other-args"
			command.OutputPaths = []string{"other-output"}
		}},
		{name: "ToolContents", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["compiler"] = persistentWorkerTestFile("new compiler", true)
		}},
		{name: "ToolExecutableBit", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["compiler"].IsExecutable = false
		}},
		{name: "ToolPath", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			inputs["other-compiler"] = inputs["compiler"]
			delete(inputs, "compiler")
		}},
		{name: "StartupArguments", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.Arguments[1] = "--different-startup"
		}},
		{name: "Environment", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.EnvironmentVariables[0].Value = "/other/bin"
		}},
		{name: "WorkingDirectory", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			command.WorkingDirectory = "subdirectory"
		}},
		{name: "ClientKey", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			action.Platform.Properties[0].Value = "different-tool-key"
		}},
		{name: "Platform", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, inputs map[string]*remoteexecution.FileNode) {
			action.Platform.Properties[2].Value = "other-platform"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			action, command, inputs := persistentWorkerTestCommand()
			digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
			first, err := builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, nil)
			require.NoError(t, err)
			if test.modify != nil {
				test.modify(action, command, inputs)
			}
			second, err := builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, nil)
			require.NoError(t, err)
			require.Equal(t, test.same, first.CompatibilityKey == second.CompatibilityKey)
		})
	}
}

func TestPersistentWorkerCommandKeyEncoding(t *testing.T) {
	action, command, inputs := persistentWorkerTestCommand()
	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	first, err := builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, map[string]string{"PATH": "/override"})
	require.NoError(t, err)
	require.Equal(t, "/override", first.EnvironmentVariables["PATH"])
	require.Equal(t, "/bin", command.EnvironmentVariables[0].Value)
	for _, otherFunction := range []digest.Function{
		digest.MustNewFunction("other-instance", remoteexecution.DigestFunction_SHA256),
		digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256TREE),
	} {
		other, err := builder.NewPersistentWorkerCommand(action.Platform, command, otherFunction, inputs, map[string]string{"PATH": "/override"})
		require.NoError(t, err)
		require.NotEqual(t, first.CompatibilityKey, other.CompatibilityKey)
	}
	other, err := builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, nil)
	require.NoError(t, err)
	require.NotEqual(t, first.CompatibilityKey, other.CompatibilityKey)
	command.Arguments = []string{"compiler", "a b", "c", "@args"}
	first, err = builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, nil)
	require.NoError(t, err)
	command.Arguments = []string{"compiler", "a", "b c", "@args"}
	other, err = builder.NewPersistentWorkerCommand(action.Platform, command, digestFunction, inputs, nil)
	require.NoError(t, err)
	require.NotEqual(t, first.CompatibilityKey, other.CompatibilityKey)
}

func TestPersistentWorkerCommandRequest(t *testing.T) {
	action, command, inputs := persistentWorkerTestCommand()
	command.Arguments = []string{"compiler", "@@startup", "@repo//label", "@args", "--flagfile=kept", "-flagfile=also-kept"}
	command.WorkingDirectory = "subdir"
	files := map[string]string{
		"subdir/args":   "@nested\r\n\r\n'quoted argument'\n--two words\n@nested\n@empty\n",
		"subdir/nested": "@@literal\n@repo//label\nlast",
		"subdir/empty":  "",
	}
	for name, contents := range files {
		inputs[name] = persistentWorkerTestFile(contents, false)
	}
	prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"compiler", "@@startup", "@repo//label", "--persistent_worker"}, prepared.Arguments)
	request, err := prepared.NewExecuteInPersistentWorkerRequest(context.Background(), "session", func(ctx context.Context, name string) ([]byte, error) {
		contents, ok := files[name]
		require.True(t, ok)
		return []byte(contents), nil
	})
	require.NoError(t, err)
	require.Equal(t, "session", request.SessionId)
	workRequest := &worker_pb.WorkRequest{}
	require.NoError(t, proto.Unmarshal(request.SerializedWorkRequest, workRequest))
	require.Equal(t, []string{"@@literal", "@repo//label", "last", "", "'quoted argument'", "--two words", "@@literal", "@repo//label", "last", "--flagfile=kept", "-flagfile=also-kept"}, workRequest.Arguments)
	require.Zero(t, workRequest.RequestId)
	require.False(t, workRequest.Cancel)
	require.Empty(t, workRequest.SandboxDir)
	var inputPaths []string
	for _, input := range workRequest.Inputs {
		inputPaths = append(inputPaths, input.Path)
		require.Equal(t, inputs[input.Path].Digest.Hash, hex.EncodeToString(input.Digest))
	}
	require.Len(t, inputPaths, len(inputs))
	require.True(t, slices.IsSorted(inputPaths))
}

func TestPersistentWorkerCommandFlagFileErrors(t *testing.T) {
	for _, contents := range []string{"@missing", "@./args", "@../outside", "@/outside", string([]byte{0xff})} {
		t.Run(contents, func(t *testing.T) {
			action, command, inputs := persistentWorkerTestCommand()
			prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
			require.NoError(t, err)
			response, err := prepared.NewExecuteInPersistentWorkerRequest(context.Background(), "session", func(ctx context.Context, name string) ([]byte, error) {
				return []byte(contents), nil
			})
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
	action, command, inputs := persistentWorkerTestCommand()
	prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
	require.NoError(t, err)
	_, err = prepared.NewExecuteInPersistentWorkerRequest(context.Background(), "session", func(ctx context.Context, name string) ([]byte, error) {
		return nil, os.ErrNotExist
	})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = prepared.NewExecuteInPersistentWorkerRequest(ctx, "session", nil)
	require.Error(t, err)
	_, err = prepared.NewExecuteInPersistentWorkerRequest(context.Background(), "", nil)
	require.Error(t, err)
	largeArgument := strings.Repeat("argument", 20000)
	request, err := prepared.NewExecuteInPersistentWorkerRequest(context.Background(), "session", func(ctx context.Context, name string) ([]byte, error) {
		return []byte(largeArgument), nil
	})
	require.NoError(t, err)
	workRequest := &worker_pb.WorkRequest{}
	require.NoError(t, proto.Unmarshal(request.SerializedWorkRequest, workRequest))
	require.Equal(t, []string{largeArgument}, workRequest.Arguments)
}

func TestDecodePersistentWorkerResponse(t *testing.T) {
	for _, workResponse := range []*worker_pb.WorkResponse{
		{},
		{ExitCode: 2, Output: "Compiler diagnostics"},
	} {
		serialized, err := proto.Marshal(workResponse)
		require.NoError(t, err)
		decoded, err := builder.DecodePersistentWorkerResponse(&runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: serialized})
		require.NoError(t, err)
		require.True(t, proto.Equal(workResponse, decoded))
	}
	for _, workResponse := range []*worker_pb.WorkResponse{{RequestId: 1}, {WasCancelled: true}} {
		serialized, err := proto.Marshal(workResponse)
		require.NoError(t, err)
		_, err = builder.DecodePersistentWorkerResponse(&runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: serialized})
		require.Error(t, err)
	}
	_, err := builder.DecodePersistentWorkerResponse(&runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: []byte{0xff}})
	require.Error(t, err)
	_, err = builder.DecodePersistentWorkerResponse(nil)
	require.Error(t, err)
}
