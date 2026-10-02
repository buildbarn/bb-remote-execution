package runner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// TestPersistentWorkerProtocolProto validates that WorkRequest messages
// are written in the same format as Bazel's writeDelimitedTo(), and
// that WorkResponse messages encoded that way can be read back.
func TestPersistentWorkerProtocolProto(t *testing.T) {
	request := &bazelworker.WorkRequest{
		Arguments: []string{"--src", "hello.java"},
		Inputs: []*bazelworker.Input{
			{
				Path:   "src/hello.java",
				Digest: []byte("5d41402abc4b2a76b9719d911017c592"),
			},
		},
	}

	t.Run("WriteWorkRequest", func(t *testing.T) {
		var output bytes.Buffer
		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_PROTO, &output, bytes.NewReader(nil))
		require.NoError(t, err)
		require.NoError(t, p.WriteWorkRequest(request))

		// The message must be prefixed with its size in bytes,
		// encoded as a Protobuf varint.
		body, err := proto.Marshal(request)
		require.NoError(t, err)
		var expected bytes.Buffer
		_, err = protodelim.MarshalTo(&expected, request)
		require.NoError(t, err)
		require.Equal(t, expected.Bytes(), output.Bytes())
		require.Equal(t, len(body)+1, output.Len())
	})

	t.Run("ReadWorkResponse", func(t *testing.T) {
		var input bytes.Buffer
		for _, response := range []*bazelworker.WorkResponse{
			{ExitCode: 0, Output: "Compiling..."},
			{ExitCode: 1, Output: "hello.java:1: error: bad"},
		} {
			_, err := protodelim.MarshalTo(&input, response)
			require.NoError(t, err)
		}

		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_PROTO, io.Discard, &input)
		require.NoError(t, err)

		response, err := p.ReadWorkResponse()
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &bazelworker.WorkResponse{
			ExitCode: 0,
			Output:   "Compiling...",
		}, response)

		response, err = p.ReadWorkResponse()
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &bazelworker.WorkResponse{
			ExitCode: 1,
			Output:   "hello.java:1: error: bad",
		}, response)

		_, err = p.ReadWorkResponse()
		require.Equal(t, io.EOF, err)
	})

	t.Run("ReadLargeWorkResponse", func(t *testing.T) {
		// protodelim applies a 4 MiB limit on messages by
		// default. Tools may emit far more output than that as
		// part of a single build action.
		var input bytes.Buffer
		_, err := protodelim.MarshalTo(&input, &bazelworker.WorkResponse{
			Output: string(bytes.Repeat([]byte{'x'}, 8*1024*1024)),
		})
		require.NoError(t, err)

		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_PROTO, io.Discard, &input)
		require.NoError(t, err)
		response, err := p.ReadWorkResponse()
		require.NoError(t, err)
		require.Len(t, response.Output, 8*1024*1024)
	})

	t.Run("OversizedWorkResponse", func(t *testing.T) {
		// A message whose length prefix exceeds the maximum must
		// be rejected before its contents are read, so that a
		// misbehaving tool cannot exhaust the memory of
		// bb_runner.
		var input bytes.Buffer
		input.Write(protowire.AppendVarint(nil, maximumWorkResponseSizeBytes+1))

		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_PROTO, io.Discard, &input)
		require.NoError(t, err)
		_, err = p.ReadWorkResponse()
		require.Error(t, err)
		require.Contains(t, err.Error(), "size")
	})

	t.Run("TruncatedWorkResponse", func(t *testing.T) {
		var complete bytes.Buffer
		_, err := protodelim.MarshalTo(&complete, &bazelworker.WorkResponse{
			Output: "Hello",
		})
		require.NoError(t, err)

		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_PROTO, io.Discard, bytes.NewReader(complete.Bytes()[:complete.Len()-1]))
		require.NoError(t, err)
		_, err = p.ReadWorkResponse()
		require.Error(t, err)
	})
}

// TestPersistentWorkerProtocolJSON validates that messages are
// exchanged as newline delimited JSON, using the canonical Protobuf
// JSON mapping. This is the format that Bazel uses for tools that
// declare the 'requires-worker-protocol=json' execution requirement.
func TestPersistentWorkerProtocolJSON(t *testing.T) {
	t.Run("WriteWorkRequest", func(t *testing.T) {
		var output bytes.Buffer
		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_JSON, &output, bytes.NewReader(nil))
		require.NoError(t, err)
		require.NoError(t, p.WriteWorkRequest(&bazelworker.WorkRequest{
			Arguments: []string{"--src", "hello.java"},
			Inputs: []*bazelworker.Input{
				{
					Path:   "src/hello.java",
					Digest: []byte("abc"),
				},
			},
		}))

		// Messages must be separated by newlines, and must not
		// contain any insignificant whitespace of their own.
		// Note that 'digest' is a bytes field, meaning that
		// Protobuf's JSON mapping requires it to be base64
		// encoded.
		require.Equal(
			t,
			"{\"arguments\":[\"--src\",\"hello.java\"],\"inputs\":[{\"path\":\"src/hello.java\",\"digest\":\"YWJj\"}]}\n",
			output.String(),
		)
	})

	t.Run("WriteMultipleWorkRequests", func(t *testing.T) {
		var output bytes.Buffer
		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_JSON, &output, bytes.NewReader(nil))
		require.NoError(t, err)
		require.NoError(t, p.WriteWorkRequest(&bazelworker.WorkRequest{}))
		require.NoError(t, p.WriteWorkRequest(&bazelworker.WorkRequest{}))
		require.Equal(t, "{}\n{}\n", output.String())
	})

	t.Run("ReadWorkResponse", func(t *testing.T) {
		// Bazel's own JSON workers are permitted to insert
		// arbitrary whitespace between messages, and may emit
		// fields that we don't know about.
		p, err := newPersistentWorkerProtocol(
			runner_pb.PersistentWorker_JSON,
			io.Discard,
			bytes.NewReader([]byte("{\"exitCode\":1,\"output\":\"Boom\"}\n\n  {\n  \"output\": \"Fine\",\n  \"someUnknownField\": 12\n}\n")),
		)
		require.NoError(t, err)

		response, err := p.ReadWorkResponse()
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &bazelworker.WorkResponse{
			ExitCode: 1,
			Output:   "Boom",
		}, response)

		response, err = p.ReadWorkResponse()
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &bazelworker.WorkResponse{
			Output: "Fine",
		}, response)

		_, err = p.ReadWorkResponse()
		require.Equal(t, io.EOF, err)
	})

	t.Run("OversizedWorkResponse", func(t *testing.T) {
		// JSON messages are not length prefixed, so the limit is
		// enforced while reading. Use a protocol instance with a
		// small limit, so that the test doesn't need to generate
		// tens of megabytes of data.
		limitReader := &resettableLimitReader{
			reader: bytes.NewReader([]byte("{\"output\":\"" + strings.Repeat("x", 1000) + "\"}\n")),
			limit:  16,
		}
		p := &jsonPersistentWorkerProtocol{
			writer:      bufio.NewWriter(io.Discard),
			limitReader: limitReader,
			decoder:     json.NewDecoder(limitReader),
		}
		_, err := p.ReadWorkResponse()
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Message exceeds maximum size of 16 bytes"),
			err,
		)
	})

	t.Run("MalformedWorkResponse", func(t *testing.T) {
		p, err := newPersistentWorkerProtocol(
			runner_pb.PersistentWorker_JSON,
			io.Discard,
			bytes.NewReader([]byte("This is not JSON\n")),
		)
		require.NoError(t, err)
		_, err = p.ReadWorkResponse()
		require.Error(t, err)
	})

	t.Run("RoundTrip", func(t *testing.T) {
		// What we write must be readable by protojson, as that
		// is what tools implementing the JSON protocol use.
		var output bytes.Buffer
		p, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_JSON, &output, bytes.NewReader(nil))
		require.NoError(t, err)
		request := &bazelworker.WorkRequest{
			Arguments:  []string{"--flagfile=params.txt"},
			RequestId:  0,
			Verbosity:  3,
			SandboxDir: "sandbox",
			Cancel:     true,
		}
		require.NoError(t, p.WriteWorkRequest(request))

		var roundTripped bazelworker.WorkRequest
		require.NoError(t, protojson.Unmarshal(bytes.TrimSuffix(output.Bytes(), []byte("\n")), &roundTripped))
		testutil.RequireEqualProto(t, request, &roundTripped)
	})
}

// TestResettableLimitReader validates the mechanism that bounds the
// size of individual JSON messages emitted by a persistent worker
// process.
func TestResettableLimitReader(t *testing.T) {
	t.Run("WithinLimit", func(t *testing.T) {
		r := &resettableLimitReader{reader: bytes.NewReader([]byte("Hello, world")), limit: 5}
		r.reset()
		buffer := make([]byte, 5)
		_, err := io.ReadFull(r, buffer)
		require.NoError(t, err)
		require.Equal(t, "Hello", string(buffer))
	})

	t.Run("ExceedsLimit", func(t *testing.T) {
		r := &resettableLimitReader{reader: bytes.NewReader([]byte("Hello, world")), limit: 5}
		r.reset()
		_, err := io.ReadAll(r)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Message exceeds maximum size of 5 bytes"),
			err,
		)
	})

	t.Run("LimitAppliesPerMessage", func(t *testing.T) {
		// Resetting must make the full budget available again,
		// so that a long lived worker process is not cut off
		// after the cumulative size of its messages exceeds the
		// limit.
		r := &resettableLimitReader{reader: bytes.NewReader([]byte("HelloWorld")), limit: 5}
		buffer := make([]byte, 5)
		for _, expected := range []string{"Hello", "World"} {
			r.reset()
			n, err := io.ReadFull(r, buffer)
			require.NoError(t, err)
			require.Equal(t, expected, string(buffer[:n]))
		}
	})
}

func TestPersistentWorkerProtocolUnsupported(t *testing.T) {
	_, err := newPersistentWorkerProtocol(runner_pb.PersistentWorker_Protocol(123), io.Discard, bytes.NewReader(nil))
	require.Error(t, err)
}
