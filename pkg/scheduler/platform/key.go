package platform

import (
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Key of a platform on which execution may take place. In addition to
// the REv2 platform properties, the key contains the instance name
// value. Each instance name may be associated with its own kinds of
// workers.
type Key struct {
	instanceNamePrefix digest.InstanceName
	platform           string
}

// NewKey creates a new Key of a platform, given an instance name and
// REv2 platform properties message.
func NewKey(instanceNamePrefix digest.InstanceName, platform *remoteexecution.Platform) (Key, error) {
	// Ensure that the platform properties are in normal form.
	if platform == nil {
		platform = &remoteexecution.Platform{}
	}

	// REv2 requires that platform properties are lexicographically
	// sorted by name and value.
	properties := platform.Properties
	for i := 1; i < len(properties); i++ {
		if properties[i-1].Name > properties[i].Name ||
			(properties[i-1].Name == properties[i].Name &&
				properties[i-1].Value >= properties[i].Value) {
			marshaler := protojson.MarshalOptions{}
			return Key{}, status.Errorf(codes.InvalidArgument, "Platform properties are not lexicographically sorted, as property %s should have been placed before property %s", marshaler.Format(properties[i]), marshaler.Format(properties[i-1]))
		}
	}

	// TODO: Switch to protojson.Marshal(). We don't want to use it
	// right now, as that will cause Prometheus metrics labels to
	// become non-deterministic. protojson.Marshal() injects random
	// whitespace into its output.
	marshaler := jsonpb.Marshaler{}
	platformString, err := marshaler.MarshalToString(platform)
	if err != nil {
		return Key{}, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal platform message")
	}
	return Key{
		instanceNamePrefix: instanceNamePrefix,
		platform:           platformString,
	}, nil
}

// MustNewKey is identical to NewKey, except that it panics upon failure.
func MustNewKey(instanceNamePrefix string, platform *remoteexecution.Platform) Key {
	key, err := NewKey(digest.MustNewInstanceName(instanceNamePrefix), platform)
	if err != nil {
		panic(err)
	}
	return key
}

// GetInstanceNamePrefix returns the instance name that was provided
// when the Key was created.
func (k Key) GetInstanceNamePrefix() digest.InstanceName {
	return k.instanceNamePrefix
}

// GetPlatformString returns the Platform message that was used to
// construct the Key in JSON form. This string is generated
// deterministically, so it is safe to use for stable comparisons, map
// keys and Prometheus metric label values.
func (k Key) GetPlatformString() string {
	return k.platform
}

// GetPlatformQueueName reobtains the instance name prefix and Platform
// message that was used to construct the Key. As this is only used
// infrequently, we don't bother keeping the unmarshalled Platform
// message around to preserve memory usage.
func (k Key) GetPlatformQueueName() *buildqueuestate.PlatformQueueName {
	var platform remoteexecution.Platform
	if err := protojson.Unmarshal([]byte(k.platform), &platform); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled platform: %s", err))
	}
	return &buildqueuestate.PlatformQueueName{
		InstanceNamePrefix: k.instanceNamePrefix.String(),
		Platform:           &platform,
	}
}
