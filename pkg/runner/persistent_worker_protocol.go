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
		return &jsonPersistentWorkerProtocol{
			writer:  bufio.NewWriter(w),
			decoder: json.NewDecoder(bufio.NewReader(r)),
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
	// Disable the size limit that protodelim applies by default, as
	// tools may emit large amounts of output as part of a single
	// WorkResponse message.
	if err := (protodelim.UnmarshalOptions{MaxSize: -1}).UnmarshalFrom(p.reader, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// jsonPersistentWorkerProtocol implements the encoding that Bazel uses
// for tools that declare the 'requires-worker-protocol=json' execution
// requirement. Messages are encoded using the canonical Protobuf JSON
// mapping and separated by newlines.
type jsonPersistentWorkerProtocol struct {
	writer  *bufio.Writer
	decoder *json.Decoder
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
