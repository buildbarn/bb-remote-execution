package runner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"
)

// maximumWorkResponseSizeBytes is the maximum size of a single
// WorkResponse message that a persistent worker process may return.
// Messages are decoded into memory in their entirety, so a limit needs
// to be imposed to prevent a single misbehaving tool from exhausting
// the memory of bb_runner, which would also affect build actions that
// run concurrently.
//
// Tools are expected to report the output of a build action through
// this message, so the limit needs to be generous.
const maximumWorkResponseSizeBytes = 64 * 1024 * 1024

// resettableLimitReader is a decorator for io.Reader that fails
// attempts to read more than a given number of bytes since the last
// call to reset(). It is used to bound the size of individual messages
// emitted by a persistent worker process, for encodings that don't
// perform length prefixing.
type resettableLimitReader struct {
	reader    io.Reader
	limit     int64
	remaining int64
}

func (r *resettableLimitReader) reset() {
	r.remaining = r.limit
}

func (r *resettableLimitReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, status.Errorf(codes.Internal, "Message exceeds maximum size of %d bytes", r.limit)
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

// persistentWorkerProtocol is used to exchange WorkRequest and
// WorkResponse messages with a single persistent worker process over
// its standard input and output. Instances are stateful, as the JSON
// implementation needs to retain data that it read ahead. They may only
// be used by a single goroutine at a time.
type persistentWorkerProtocol interface {
	WriteWorkRequest(request *bazelworker.WorkRequest) error
	ReadWorkResponse() (*bazelworker.WorkResponse, error)
}

// newPersistentWorkerProtocol creates a persistentWorkerProtocol that
// uses the encoding requested by the client.
func newPersistentWorkerProtocol(protocol runner_pb.PersistentWorker_Protocol, w io.Writer, r io.Reader) (persistentWorkerProtocol, error) {
	switch protocol {
	case runner_pb.PersistentWorker_PROTO:
		return &protoPersistentWorkerProtocol{
			writer: bufio.NewWriter(w),
			reader: bufio.NewReader(r),
		}, nil
	case runner_pb.PersistentWorker_JSON:
		limitReader := &resettableLimitReader{
			reader: bufio.NewReader(r),
			limit:  maximumWorkResponseSizeBytes,
		}
		return &jsonPersistentWorkerProtocol{
			writer:      bufio.NewWriter(w),
			limitReader: limitReader,
			decoder:     json.NewDecoder(limitReader),
		}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported persistent worker protocol %#v", protocol.String())
	}
}

// protoPersistentWorkerProtocol implements the default encoding used by
// Bazel, in which every message is prefixed with its size in bytes,
// encoded as a Protobuf varint.
type protoPersistentWorkerProtocol struct {
	writer *bufio.Writer
	reader *bufio.Reader
}

func (p *protoPersistentWorkerProtocol) WriteWorkRequest(request *bazelworker.WorkRequest) error {
	if _, err := protodelim.MarshalTo(p.writer, request); err != nil {
		return err
	}
	return p.writer.Flush()
}

func (p *protoPersistentWorkerProtocol) ReadWorkResponse() (*bazelworker.WorkResponse, error) {
	var response bazelworker.WorkResponse
	// Raise the 4 MiB size limit that protodelim applies by default,
	// as tools may emit large amounts of output as part of a single
	// WorkResponse message.
	if err := (protodelim.UnmarshalOptions{MaxSize: maximumWorkResponseSizeBytes}).UnmarshalFrom(p.reader, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// jsonPersistentWorkerProtocol implements the encoding that Bazel uses
// for tools that declare the 'requires-worker-protocol=json' execution
// requirement. Messages are encoded using the canonical Protobuf JSON
// mapping and separated by newlines.
type jsonPersistentWorkerProtocol struct {
	writer      *bufio.Writer
	limitReader *resettableLimitReader
	decoder     *json.Decoder
}

func (p *jsonPersistentWorkerProtocol) WriteWorkRequest(request *bazelworker.WorkRequest) error {
	data, err := protojson.Marshal(request)
	if err != nil {
		return err
	}
	// protojson deliberately injects random whitespace into its
	// output to prevent callers from depending on its exact
	// formatting. Remove it, so that what we send to the worker
	// process is reproducible.
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, data); err != nil {
		return err
	}
	if _, err := p.writer.Write(compacted.Bytes()); err != nil {
		return err
	}
	if err := p.writer.WriteByte('\n'); err != nil {
		return err
	}
	return p.writer.Flush()
}

func (p *jsonPersistentWorkerProtocol) ReadWorkResponse() (*bazelworker.WorkResponse, error) {
	// Bound the amount of data that may be read while decoding a
	// single message. Note that the decoder buffers data internally,
	// meaning the effective limit is the one below plus the size of
	// its buffer.
	p.limitReader.reset()

	var message json.RawMessage
	if err := p.decoder.Decode(&message); err != nil {
		return nil, err
	}
	var response bazelworker.WorkResponse
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(message, &response); err != nil {
		return nil, err
	}
	return &response, nil
}
