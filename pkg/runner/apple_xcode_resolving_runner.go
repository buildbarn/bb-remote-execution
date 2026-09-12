package runner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os/exec"
	"sort"
	"strings"
	"sync"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// Environment variables provided by Bazel.
	environmentVariableXcodeVersion = "XCODE_VERSION_OVERRIDE"
	environmentVariableSDKPlatform  = "APPLE_SDK_PLATFORM"

	// Environment variables provided to the build action.
	environmentVariableDeveloperDirectory = "DEVELOPER_DIR"
	environmentVariableSDKRoot            = "SDKROOT"
)

// AppleXcodeSDKRootResolver is a callback function that is used to
// obtain the path of an Xcode SDK root directory (SDKROOT), given a
// developer directory (DEVELOPER_DIR) and an SDK name.
type AppleXcodeSDKRootResolver func(ctx context.Context, developerDirectory, sdkName string) (string, error)

// LocalAppleXcodeSDKRootResolver resolves the SDK root directory
// (SDKROOT) by calling into the xcrun utility on the current system.
func LocalAppleXcodeSDKRootResolver(ctx context.Context, developerDirectory, sdkName string) (string, error) {
	cmd := exec.CommandContext(ctx, "/usr/bin/xcrun", "--sdk", sdkName, "--show-sdk-path")
	cmd.Env = []string{environmentVariableDeveloperDirectory + "=" + developerDirectory}
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr := string(exitErr.Stderr)
			code := codes.FailedPrecondition
			if strings.Contains(stderr, "Input/output error") {
				// If xcrun fails to read files from disk,
				// we should return a retriable error.
				code = codes.Internal
			}
			return "", util.StatusWrapfWithCode(err, code, "xcrun failed with output %#v", stderr)
		}
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// NewCachingAppleXcodeSDKRootResolver creates a decorator for
// AppleXcodeSDKRootResolver that caches successful results of
// successive calls. As it is assumed that the number of SDKs installed
// on the current system is small, no bounds are placed on the maximum
// cache size.
func NewCachingAppleXcodeSDKRootResolver(base AppleXcodeSDKRootResolver) AppleXcodeSDKRootResolver {
	type cacheKey struct {
		developerDirectory string
		sdkName            string
	}
	var lock sync.Mutex
	cache := map[cacheKey]string{}

	return func(ctx context.Context, developerDirectory, sdkName string) (string, error) {
		key := cacheKey{
			developerDirectory: developerDirectory,
			sdkName:            sdkName,
		}
		lock.Lock()
		sdkRoot, ok := cache[key]
		lock.Unlock()
		if ok {
			return sdkRoot, nil
		}

		sdkRoot, err := base(ctx, developerDirectory, sdkName)
		if err != nil {
			return "", err
		}

		lock.Lock()
		cache[key] = sdkRoot
		lock.Unlock()
		return sdkRoot, nil
	}
}

type appleXcodeResolvingRunner struct {
	runner_pb.RunnerServer
	appleXcodeEnvironmentResolver
}

type appleXcodeEnvironmentResolver struct {
	developerDirectories map[string]string
	supportedVersions    string
	sdkRootResolver      AppleXcodeSDKRootResolver
}

// NewAppleXcodeResolvingRunner creates a decorator for RunnerServer
// that injects DEVELOPER_DIR and SDKROOT environment variables into
// actions, based on the presence of APPLE_SDK_PLATFORM and
// XCODE_VERSION_OVERRIDE environment variables.
//
// This decorator can be used on macOS workers to let build actions
// choose between one of the copies of Xcode that is installed on the
// worker, without requiring that the client hardcodes the absolute path
// at which Xcode is installed.
//
// This decorator implements the convention that is used by Bazel. For
// local execution, Bazel implements similar logic as part of class
// com.google.devtools.build.lib.exec.local.XcodeLocalEnvProvider.
func NewAppleXcodeResolvingRunner(base runner_pb.RunnerServer, developerDirectories map[string]string, sdkRootResolver AppleXcodeSDKRootResolver) runner_pb.RunnerServer {
	return &appleXcodeResolvingRunner{
		RunnerServer:                  base,
		appleXcodeEnvironmentResolver: newAppleXcodeEnvironmentResolver(developerDirectories, sdkRootResolver),
	}
}

func newAppleXcodeEnvironmentResolver(developerDirectories map[string]string, sdkRootResolver AppleXcodeSDKRootResolver) appleXcodeEnvironmentResolver {
	// Create a sorted list of all Xcode versions, to display as
	// part of error messages.
	supportedVersions := make([]string, 0, len(developerDirectories))
	for supportedVersion := range developerDirectories {
		supportedVersions = append(supportedVersions, supportedVersion)
	}
	sort.Strings(supportedVersions)

	return appleXcodeEnvironmentResolver{
		developerDirectories: developerDirectories,
		supportedVersions:    fmt.Sprintf("%v", supportedVersions),
		sdkRootResolver:      sdkRootResolver,
	}
}

func (r *appleXcodeResolvingRunner) Run(ctx context.Context, oldRequest *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	environment, err := r.resolveEnvironment(ctx, oldRequest.EnvironmentVariables)
	if err != nil {
		return nil, err
	}
	if environment == nil {
		return r.RunnerServer.Run(ctx, oldRequest)
	}
	newRequest := proto.Clone(oldRequest).(*runner_pb.RunRequest)
	newRequest.EnvironmentVariables = environment
	return r.RunnerServer.Run(ctx, newRequest)
}

func (resolver *appleXcodeEnvironmentResolver) resolveEnvironment(ctx context.Context, oldEnvironmentVariables map[string]string) (map[string]string, error) {
	// Check whether we need to infer DEVELOPER_DIR from
	// XCODE_VERSION_OVERRIDE.
	_, hasDeveloperDir := oldEnvironmentVariables[environmentVariableDeveloperDirectory]
	xcodeVersion, hasXcodeVersion := oldEnvironmentVariables[environmentVariableXcodeVersion]
	if hasDeveloperDir || !hasXcodeVersion {
		return nil, nil
	}

	developerDir, ok := resolver.developerDirectories[xcodeVersion]
	if !ok {
		// Bazel 8.3 and later also allow XCODE_VERSION_OVERRIDE
		// to be set to a DEVELOPER_DIR path. This can be used
		// to force the use of a specific copy of Xcode.
		developerDirBuilder, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
		if err := path.Resolve(path.UNIXFormat.NewParser(xcodeVersion), scopeWalker); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "Attempted to use Xcode installation with version %#v, while only %s are supported", xcodeVersion, resolver.supportedVersions)
		}
		developerDir = developerDirBuilder.GetUNIXString()
	}

	newEnvironment := maps.Clone(oldEnvironmentVariables)
	newEnvironment[environmentVariableDeveloperDirectory] = developerDir

	// Check whether we need to infer SDKROOT from APPLE_SDK_PLATFORM.
	_, hasSDKRoot := oldEnvironmentVariables[environmentVariableSDKRoot]
	sdkPlatform, hasSDKPlatform := oldEnvironmentVariables[environmentVariableSDKPlatform]
	if !hasSDKRoot && hasSDKPlatform {
		sdkName := strings.ToLower(sdkPlatform)
		sdkRoot, err := resolver.sdkRootResolver(ctx, developerDir, sdkName)
		if err != nil {
			return nil, util.StatusWrapf(err, "Cannot resolve root for SDK %#v in Xcode developer directory %#v", sdkName, developerDir)
		}
		newEnvironment[environmentVariableSDKRoot] = sdkRoot
	}

	return newEnvironment, nil
}

type appleXcodeResolvingPersistentRunner struct {
	runner_pb.PersistentRunnerServer
	appleXcodeEnvironmentResolver
}

// NewAppleXcodeResolvingPersistentRunner resolves Xcode environment
// variables before starting a persistent compiler, using the same rules
// as ordinary execution. Session requests retain the resolved environment.
func NewAppleXcodeResolvingPersistentRunner(base runner_pb.PersistentRunnerServer, developerDirectories map[string]string, sdkRootResolver AppleXcodeSDKRootResolver) runner_pb.PersistentRunnerServer {
	return &appleXcodeResolvingPersistentRunner{
		PersistentRunnerServer:        base,
		appleXcodeEnvironmentResolver: newAppleXcodeEnvironmentResolver(developerDirectories, sdkRootResolver),
	}
}

func (server *appleXcodeResolvingPersistentRunner) CreateSession(ctx context.Context, request *runner_pb.CreateSessionRequest) (*runner_pb.CreateSessionResponse, error) {
	environment, err := server.resolveEnvironment(ctx, request.EnvironmentVariables)
	if err != nil {
		return nil, err
	}
	if environment == nil {
		return server.PersistentRunnerServer.CreateSession(ctx, request)
	}
	resolved := proto.Clone(request).(*runner_pb.CreateSessionRequest)
	resolved.EnvironmentVariables = environment
	return server.PersistentRunnerServer.CreateSession(ctx, resolved)
}
