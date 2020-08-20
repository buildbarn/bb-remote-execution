package builder

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
)

func withMessagingAttributes(key platformKey, digest *remoteexecution.Digest) trace.StartOption {
	return trace.WithAttributes(
		label.String("messaging.system", "bb_scheduler"),
		label.String("messaging.destination", key.String()),
		label.String("messaging.message_id", digest.Hash),
	)
}
