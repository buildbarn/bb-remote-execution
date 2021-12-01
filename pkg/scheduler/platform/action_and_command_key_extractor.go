package platform

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	actionAndCommandKeyExtractorPrometheusMetrics sync.Once

	actionAndCommandKeyExtractorCommandMessagesReadTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "action_and_command_key_extractor_command_messages_read_total",
			Help:      "Number of times REv2 Action messages did not contain platform properties, meaning Command messages had to be loaded instead.",
		})
)

type actionAndCommandKeyExtractor struct {
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
}

// NewActionAndCommandKeyExtractor creates a new KeyExtractor is capable
// of extracting a platform key from an REv2 Action message. If no
// platform properties are specified in the Action, it falls back to
// reading them from the Command message.
//
// This platform key extractor needs to be used if requests from clients
// that implement REv2.1 or older need to be processed, as platform
// properties were only added to the Action message in REv2.2.
func NewActionAndCommandKeyExtractor(contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int) KeyExtractor {
	actionAndCommandKeyExtractorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(actionAndCommandKeyExtractorCommandMessagesReadTotal)
	})

	return &actionAndCommandKeyExtractor{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (ke *actionAndCommandKeyExtractor) ExtractKey(ctx context.Context, instanceName digest.InstanceName, action *remoteexecution.Action) (Key, error) {
	if action.Platform != nil {
		// REv2.2 or newer: platform properties are stored in
		// the Action message.
		key, err := NewKey(instanceName, action.Platform)
		if err != nil {
			return Key{}, util.StatusWrap(err, "Failed to extract platform key from action")
		}
		return key, nil
	}

	// REv2.1 or older: platform properties are stored in the
	// Command message.
	commandDigest, err := instanceName.NewDigestFromProto(action.CommandDigest)
	if err != nil {
		return Key{}, util.StatusWrap(err, "Failed to extract digest for command")
	}
	commandMessage, err := ke.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, ke.maximumMessageSizeBytes)
	if err != nil {
		return Key{}, util.StatusWrap(err, "Failed to obtain command")
	}
	command := commandMessage.(*remoteexecution.Command)
	key, err := NewKey(instanceName, command.Platform)
	if err != nil {
		return Key{}, util.StatusWrap(err, "Failed to extract platform key from command")
	}
	actionAndCommandKeyExtractorCommandMessagesReadTotal.Inc()
	return key, nil
}
