package runner

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"sync"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// Environment variables provided by Bazel.
	environmentVariableXcodeVersion = "XCODE_VERSION_OVERRIDE"
	environmentVariableSDKPlatform  = "APPLE_SDK_PLATFORM"
	environmentVariableSDKVersion   = "APPLE_SDK_VERSION_OVERRIDE"

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
			return "", status.Errorf(codes.FailedPrecondition, "xcrun failed with output %#v", string(exitErr.Stderr))
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
	developerDirectories map[string]string
	supportedVersions    string
	sdkRootResolver      AppleXcodeSDKRootResolver
}

// NewAppleXcodeResolvingRunner creates a decorator for RunnerServer
// that injects DEVELOPER_DIR and SDKROOT environment variables into
// actions, based on the presence of APPLE_SDK_PLATFORM,
// APPLE_SDK_VERSION_OVERRIDE, and XCODE_VERSION_OVERRIDE environment
// variables.
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
	// Create a sorted list of all Xcode versions, to display as
	// part of error messages.
	supportedVersions := make([]string, 0, len(developerDirectories))
	for supportedVersion := range developerDirectories {
		supportedVersions = append(supportedVersions, supportedVersion)
	}
	sort.Strings(supportedVersions)

	return &appleXcodeResolvingRunner{
		RunnerServer:         base,
		developerDirectories: developerDirectories,
		supportedVersions:    fmt.Sprintf("%v", supportedVersions),
		sdkRootResolver:      sdkRootResolver,
	}
}

func (r *appleXcodeResolvingRunner) Run(ctx context.Context, oldRequest *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	// Check whether we need to infer DEVELOPER_DIR from
	// XCODE_VERSION_OVERRIDE.
	oldEnvironmentVariables := oldRequest.EnvironmentVariables
	_, hasDeveloperDir := oldEnvironmentVariables[environmentVariableDeveloperDirectory]
	xcodeVersion, hasXcodeVersion := oldEnvironmentVariables[environmentVariableXcodeVersion]
	if hasDeveloperDir || !hasXcodeVersion {
		return r.RunnerServer.Run(ctx, oldRequest)
	}

	developerDir, ok := r.developerDirectories[xcodeVersion]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "Attempted to use Xcode installation with version %#v, while only %s are supported", xcodeVersion, r.supportedVersions)
	}

	var newRequest runner_pb.RunRequest
	proto.Merge(&newRequest, oldRequest)
	newEnvironment := newRequest.EnvironmentVariables
	newEnvironment[environmentVariableDeveloperDirectory] = developerDir

	// Check whether we need to infer SDKROOT from
	// APPLE_SDK_PLATFORM and APPLE_SDK_VERSION_OVERRIDE.
	_, hasSDKRoot := oldEnvironmentVariables[environmentVariableSDKRoot]
	sdkPlatform, hasSDKPlatform := oldEnvironmentVariables[environmentVariableSDKPlatform]
	sdkVersion, hasSDKVersion := oldEnvironmentVariables[environmentVariableSDKVersion]
	if !hasSDKRoot && hasSDKPlatform && hasSDKVersion {
		sdkName := strings.ToLower(sdkPlatform) + sdkVersion
		sdkRoot, err := r.sdkRootResolver(ctx, developerDir, sdkName)
		if err != nil {
			return nil, util.StatusWrapf(err, "Cannot resolve root for SDK %#v in Xcode developer directory %#v", sdkName, developerDir)
		}
		newEnvironment[environmentVariableSDKRoot] = sdkRoot
	}

	return r.RunnerServer.Run(ctx, &newRequest)
}
