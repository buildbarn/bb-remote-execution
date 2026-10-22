package runner

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// PersistentWorkerAdapter exchanges opaque protobuf payloads with a persistent worker.
type PersistentWorkerAdapter struct {
	stdin                    io.Writer
	stdout                   *bufio.Reader
	maximumResponseSizeBytes uint64
}

// NewPersistentWorkerAdapter creates an adapter for exchanging opaque
// protobuf payloads over a worker's stdin and stdout. Responses larger
// than the configured limit are rejected before allocating a buffer.
// The adapter must be retained across exchanges to preserve buffered data.
func NewPersistentWorkerAdapter(stdin io.Writer, stdout io.Reader, maximumResponseSizeBytes uint64) *PersistentWorkerAdapter {
	return &PersistentWorkerAdapter{
		stdin:                    stdin,
		stdout:                   bufio.NewReader(stdout),
		maximumResponseSizeBytes: maximumResponseSizeBytes,
	}
}

// WriteRequest writes a request payload preceded by its varint-encoded
// length. The payload is forwarded without decoding it.
func (protocol *PersistentWorkerAdapter) WriteRequest(request []byte) error {
	var header [binary.MaxVarintLen64]byte
	headerSize := binary.PutUvarint(header[:], uint64(len(request)))
	written, err := protocol.stdin.Write(header[:headerSize])
	if err != nil {
		return err
	}
	if written != headerSize {
		return io.ErrShortWrite
	}
	if len(request) == 0 {
		return nil
	}
	written, err = protocol.stdin.Write(request)
	if err == nil && written != len(request) {
		return io.ErrShortWrite
	}
	return err
}

// ReadResponse reads a length-prefixed response and returns its payload
// without decoding it. Invalid lengths, oversized responses, and
// incomplete frames result in an error.
func (protocol *PersistentWorkerAdapter) ReadResponse() ([]byte, error) {
	size, err := binary.ReadUvarint(protocol.stdout)
	if err != nil {
		return nil, err
	}
	if size > protocol.maximumResponseSizeBytes {
		return nil, fmt.Errorf("persistent worker response size %d exceeds maximum %d bytes", size, protocol.maximumResponseSizeBytes)
	}
	if size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("persistent worker response size %d exceeds maximum buffer size", size)
	}
	response := make([]byte, int(size))
	if _, err := io.ReadFull(protocol.stdout, response); err != nil {
		return nil, err
	}
	return response, nil
}
