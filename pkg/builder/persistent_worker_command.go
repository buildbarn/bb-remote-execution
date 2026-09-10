package builder

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	worker_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PersistentWorkerCommand contains the startup configuration,
// compatibility key, and per-request inputs for a persistent compiler.
type PersistentWorkerCommand struct {
	Arguments            []string
	EnvironmentVariables map[string]string
	WorkingDirectory     string
	CompatibilityKey     [sha256.Size]byte
	requestArguments     []string
	inputs               []*worker_pb.Input
	inputPaths           map[string]struct{}
}

// NewPersistentWorkerCommand prepares an REv2 command for singleplex execution.
//
// Inputs must contain the complete file manifest, keyed by canonical
// input-root-relative paths, with tool files marked by bazel_tool_input.
// Unsupported actions or actions without marked tools return (nil, nil)
func NewPersistentWorkerCommand(action *remoteexecution.Action, command *remoteexecution.Command, digestFunction digest.Function, inputs map[string]*remoteexecution.FileNode, environmentVariables map[string]string) (*PersistentWorkerCommand, error) {
	platform := action.GetPlatform()
	if platform == nil {
		platform = command.GetPlatform()
	}
	properties := map[string]string{}
	for _, property := range platform.GetProperties() {
		if property.GetName() == "persistentWorkerKey" || property.GetName() == "persistentWorkerProtocol" {
			if _, ok := properties[property.Name]; ok {
				return nil, status.Errorf(codes.InvalidArgument, "Duplicate platform property %q", property.Name)
			}
			properties[property.Name] = property.Value
		}
	}
	if properties["persistentWorkerKey"] == "" {
		return nil, nil
	}

  // TODO: support JSON as well. 
	if protocol := properties["persistentWorkerProtocol"]; protocol != "" && protocol != "proto" {
		return nil, nil
	}
	if len(command.GetArguments()) == 0 || command.Arguments[0] == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing persistent worker executable")
	}
	workingDirectory := path.Clean(command.WorkingDirectory)
	if path.IsAbs(workingDirectory) || workingDirectory == ".." || strings.HasPrefix(workingDirectory, "../") {
		return nil, status.Error(codes.InvalidArgument, "Working directory is outside the input root")
	}
	if workingDirectory == "." {
		workingDirectory = ""
	}
	prepared := &PersistentWorkerCommand{
		Arguments:            []string{command.Arguments[0], "--persistent_worker"},
		EnvironmentVariables: map[string]string{},
		WorkingDirectory:     workingDirectory,
		inputPaths:           map[string]struct{}{},
	}
	for _, argument := range command.Arguments[1:] {
		if isPersistentWorkerFlagFile(argument) {
			prepared.requestArguments = append(prepared.requestArguments, argument)
		} else {
			prepared.Arguments = append(prepared.Arguments, argument)
		}
	}
	if len(prepared.requestArguments) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing persistent worker flag file")
	}
	for _, variable := range command.EnvironmentVariables {
		if _, ok := prepared.EnvironmentVariables[variable.GetName()]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Duplicate environment variable %q", variable.GetName())
		}
		prepared.EnvironmentVariables[variable.GetName()] = variable.GetValue()
	}
	maps.Copy(prepared.EnvironmentVariables, environmentVariables)

	toolDirectory := &remoteexecution.Directory{}
	for _, inputPath := range slices.Sorted(maps.Keys(inputs)) {
		if !validPersistentWorkerInputPath(inputPath) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid input path %q", inputPath)
		}
		file := inputs[inputPath]
		if _, err := digestFunction.NewDigestFromProto(file.GetDigest()); err != nil {
			return nil, util.StatusWrapf(err, "Invalid digest for input %q", inputPath)
		}
		hashBytes, err := hex.DecodeString(file.Digest.Hash)
		if err != nil {
			return nil, err
		}
		prepared.inputs = append(prepared.inputs, &worker_pb.Input{Path: inputPath, Digest: hashBytes})
		prepared.inputPaths[inputPath] = struct{}{}
		for _, property := range file.GetNodeProperties().GetProperties() {
			if property.GetName() == "bazel_tool_input" {
				toolDirectory.Files = append(toolDirectory.Files, &remoteexecution.FileNode{
					Name:         inputPath,
					Digest:       proto.Clone(file.Digest).(*remoteexecution.Digest),
					IsExecutable: file.IsExecutable,
				})
				break
			}
		}
	}
	if len(toolDirectory.Files) == 0 {
		return nil, nil
	}

	compatibilityKey, err := computePersistentWorkerCompatibilityKey(prepared, platform, digestFunction, toolDirectory)
	if err != nil {
		return nil, err
	}
	prepared.CompatibilityKey = compatibilityKey
	return prepared, nil
}

func computePersistentWorkerCompatibilityKey(prepared *PersistentWorkerCommand, platform *remoteexecution.Platform, digestFunction digest.Function, toolDirectory *remoteexecution.Directory) ([sha256.Size]byte, error) {
	canonicalCommand := &remoteexecution.Command{
		Arguments:        prepared.Arguments,
		WorkingDirectory: prepared.WorkingDirectory,
		Platform:         proto.Clone(platform).(*remoteexecution.Platform),
	}
	slices.SortFunc(canonicalCommand.Platform.Properties, func(first, second *remoteexecution.Platform_Property) int {
		if compared := strings.Compare(first.GetName(), second.GetName()); compared != 0 {
			return compared
		}
		return strings.Compare(first.GetValue(), second.GetValue())
	})
	for _, name := range slices.Sorted(maps.Keys(prepared.EnvironmentVariables)) {
		canonicalCommand.EnvironmentVariables = append(canonicalCommand.EnvironmentVariables, &remoteexecution.Command_EnvironmentVariable{
			Name: name, Value: prepared.EnvironmentVariables[name],
		})
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	commandBytes, err := marshal.Marshal(canonicalCommand)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	toolBytes, err := marshal.Marshal(toolDirectory)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	var keyBytes []byte
	for _, field := range [][]byte{
		[]byte(digestFunction.GetInstanceName().String()),
		[]byte(strconv.FormatInt(int64(digestFunction.GetEnumValue()), 10)),
		commandBytes,
		toolBytes,
	} {
		keyBytes = binary.AppendUvarint(keyBytes, uint64(len(field)))
		keyBytes = append(keyBytes, field...)
	}
	return sha256.Sum256(keyBytes), nil
}

func isPersistentWorkerFlagFile(argument string) bool {
	return (len(argument) > 1 && strings.HasPrefix(argument, "@") &&
		!strings.HasPrefix(argument, "@@") && !strings.Contains(argument, "//")) ||
		(strings.HasPrefix(argument, "--flagfile=") && len(argument) > len("--flagfile=")) ||
		(strings.HasPrefix(argument, "-flagfile=") && len(argument) > len("-flagfile="))
}

func validPersistentWorkerInputPath(inputPath string) bool {
	return inputPath != "" && inputPath != "." && inputPath != ".." &&
		!path.IsAbs(inputPath) && !strings.HasPrefix(inputPath, "../") &&
		path.Clean(inputPath) == inputPath && !strings.ContainsRune(inputPath, '\x00')
}

// NewExecuteSessionRequest expands flag files and serializes WorkRequest. 
// This method constructs the request without sending an RPC.
func (prepared *PersistentWorkerCommand) NewExecuteSessionRequest(ctx context.Context, sessionID string, readFile func(context.Context, string) ([]byte, error)) (*runner_pb.ExecuteSessionRequest, error) {
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing session ID")
	}
	request := &worker_pb.WorkRequest{Inputs: prepared.inputs}
	activeFiles := map[string]bool{}
	var expand func(string) error
	expand = func(argument string) error {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if len(argument) <= 1 || !strings.HasPrefix(argument, "@") ||
			strings.HasPrefix(argument, "@@") || strings.Contains(argument, "//") {
			request.Arguments = append(request.Arguments, argument)
			return nil
		}
		if path.IsAbs(argument[1:]) {
			return status.Error(codes.InvalidArgument, "Flag file is outside the input root")
		}
		filePath := path.Join(prepared.WorkingDirectory, argument[1:])
		if _, ok := prepared.inputPaths[filePath]; !ok {
			return status.Errorf(codes.InvalidArgument, "Flag file %q is not a declared input", filePath)
		}
		if activeFiles[filePath] {
			return status.Errorf(codes.InvalidArgument, "Cyclic flag file reference to %q", filePath)
		}
		activeFiles[filePath] = true
		defer delete(activeFiles, filePath)
		contents, err := readFile(ctx, filePath)
		if err != nil {
			return util.StatusWrapf(err, "Failed to read flag file %q", filePath)
		}
		lines := strings.ReplaceAll(strings.ReplaceAll(string(contents), "\r\n", "\n"), "\r", "\n")
		if lines == "" {
			return nil
		}
		for _, line := range strings.Split(strings.TrimSuffix(lines, "\n"), "\n") {
			if err := expand(line); err != nil {
				return err
			}
		}
		return nil
	}
	for _, argument := range prepared.requestArguments {
		if err := expand(argument); err != nil {
			return nil, err
		}
	}
	serialized, err := proto.Marshal(request)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to serialize persistent work request")
	}
	return &runner_pb.ExecuteSessionRequest{SessionId: sessionID, SerializedWorkRequest: serialized}, nil
}

// DecodePersistentWorkerResponse decodes and validates a response to a work request. 
// A nonzero compiler exit code is a valid response, not an error.
// The caller must retire the session if decoding or protocol validation fails.
func DecodePersistentWorkerResponse(response *runner_pb.ExecuteSessionResponse) (*worker_pb.WorkResponse, error) {
	if response == nil {
		return nil, status.Error(codes.DataLoss, "Missing persistent work response")
	}
	workResponse := &worker_pb.WorkResponse{}
	if err := proto.Unmarshal(response.SerializedWorkResponse, workResponse); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.DataLoss, "Invalid persistent work response")
	}
	if workResponse.RequestId != 0 || workResponse.WasCancelled {
		return nil, status.Error(codes.DataLoss, "Unexpected persistent work response for a singleplex request")
	}
	return workResponse, nil
}
