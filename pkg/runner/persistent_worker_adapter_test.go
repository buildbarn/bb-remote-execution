package runner_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
	"testing/iotest"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/stretchr/testify/require"
)

func TestPersistentWorkerAdapterWriteRequest(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
		frame   []byte
	}{
		{
			name:  "Empty",
			frame: []byte{0},
		},
		{
			name:    "OpaquePayload",
			payload: []byte{0xff, 0x00, 0x80},
			frame:   []byte{3, 0xff, 0x00, 0x80},
		},
		{
			name:    "MultibyteLength",
			payload: bytes.Repeat([]byte{0xff}, 300),
			frame:   append([]byte{0xac, 0x02}, bytes.Repeat([]byte{0xff}, 300)...),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdin bytes.Buffer
			protocol := runner.NewPersistentWorkerAdapter(&stdin, bytes.NewReader(nil), 0)
			require.NoError(t, protocol.WriteRequest(test.payload))
			require.Equal(t, test.frame, stdin.Bytes())
		})
	}
}

type persistentWorkerWriterFunc func([]byte) (int, error)

func (writer persistentWorkerWriterFunc) Write(data []byte) (int, error) {
	return writer(data)
}

func TestPersistentWorkerAdapterWriteRequestFailure(t *testing.T) {
	writeError := errors.New("write failed")
	for _, test := range []struct {
		name       string
		failedCall int
		writeError error
		expected   error
	}{
		{name: "HeaderError", failedCall: 1, writeError: writeError, expected: writeError},
		{name: "PayloadError", failedCall: 2, writeError: writeError, expected: writeError},
		{name: "HeaderShortWrite", failedCall: 1, expected: io.ErrShortWrite},
		{name: "PayloadShortWrite", failedCall: 2, expected: io.ErrShortWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			stdin := persistentWorkerWriterFunc(func(data []byte) (int, error) {
				calls++
				if calls == test.failedCall {
					return len(data) - 1, test.writeError
				}
				return len(data), nil
			})
			protocol := runner.NewPersistentWorkerAdapter(stdin, bytes.NewReader(nil), 0)
			require.ErrorIs(t, protocol.WriteRequest([]byte{0xff, 0x00, 0x80}), test.expected)
			require.Equal(t, test.failedCall, calls)
		})
	}
}

func TestPersistentWorkerAdapterReadResponse(t *testing.T) {
	for _, test := range []struct {
		name    string
		frame   []byte
		payload []byte
	}{
		{
			name:    "Empty",
			frame:   []byte{0},
			payload: []byte{},
		},
		{
			name:    "OpaquePayload",
			frame:   []byte{3, 0xff, 0x00, 0x80},
			payload: []byte{0xff, 0x00, 0x80},
		},
		{
			name:    "MultibyteLength",
			frame:   append([]byte{0xac, 0x02}, bytes.Repeat([]byte{0xff}, 300)...),
			payload: bytes.Repeat([]byte{0xff}, 300),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			protocol := runner.NewPersistentWorkerAdapter(io.Discard, iotest.OneByteReader(bytes.NewReader(test.frame)), uint64(len(test.payload)))
			response, err := protocol.ReadResponse()
			require.NoError(t, err)
			require.Equal(t, test.payload, response)
		})
	}
}

func TestPersistentWorkerAdapterConsecutiveFrames(t *testing.T) {
	var stdin bytes.Buffer
	protocol := runner.NewPersistentWorkerAdapter(&stdin, bytes.NewReader([]byte{2, 0xff, 0x00, 0, 1, 0x80}), 2)
	for _, payload := range [][]byte{{0xff, 0x00}, {}, {0x80}} {
		require.NoError(t, protocol.WriteRequest(payload))
		response, err := protocol.ReadResponse()
		require.NoError(t, err)
		require.Equal(t, payload, response)
	}
	response, err := protocol.ReadResponse()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, response)
	require.Equal(t, []byte{2, 0xff, 0x00, 0, 1, 0x80}, stdin.Bytes())
}

func TestPersistentWorkerAdapterReadResponseFailure(t *testing.T) {
	readError := errors.New("read failed")
	for _, test := range []struct {
		name     string
		stdout   io.Reader
		expected error
	}{
		{name: "EOF", stdout: bytes.NewReader(nil), expected: io.EOF},
		{name: "TruncatedHeader", stdout: bytes.NewReader([]byte{0x80}), expected: io.ErrUnexpectedEOF},
		{name: "MissingPayload", stdout: bytes.NewReader([]byte{3}), expected: io.EOF},
		{name: "TruncatedPayload", stdout: bytes.NewReader([]byte{3, 0xff}), expected: io.ErrUnexpectedEOF},
		{name: "HeaderReadError", stdout: iotest.ErrReader(readError), expected: readError},
		{name: "PayloadReadError", stdout: io.MultiReader(bytes.NewReader([]byte{3, 0xff}), iotest.ErrReader(readError)), expected: readError},
	} {
		t.Run(test.name, func(t *testing.T) {
			protocol := runner.NewPersistentWorkerAdapter(io.Discard, test.stdout, 300)
			response, err := protocol.ReadResponse()
			require.ErrorIs(t, err, test.expected)
			require.Nil(t, response)
		})
	}
}

func TestPersistentWorkerAdapterReadResponseInvalidLength(t *testing.T) {
	for _, header := range [][]byte{
		bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64),
		append(bytes.Repeat([]byte{0xff}, binary.MaxVarintLen64-1), 0x02),
	} {
		protocol := runner.NewPersistentWorkerAdapter(io.Discard, bytes.NewReader(header), math.MaxUint64)
		response, err := protocol.ReadResponse()
		require.Error(t, err)
		require.Nil(t, response)
	}
}

func TestPersistentWorkerAdapterReadResponseSizeLimit(t *testing.T) {
	for _, test := range []struct {
		name    string
		size    uint64
		maximum uint64
	}{
		{name: "OverLimit", size: 301, maximum: 300},
		{name: "ZeroLimit", size: 1, maximum: 0},
		{name: "HugeLength", size: math.MaxUint64, maximum: 300},
		{name: "BufferSizeOverflow", size: math.MaxUint64, maximum: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			stdout := bytes.NewReader(append(binary.AppendUvarint(nil, test.size), 0xff))
			protocol := runner.NewPersistentWorkerAdapter(io.Discard, iotest.OneByteReader(stdout), test.maximum)
			response, err := protocol.ReadResponse()
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, 1, stdout.Len())
		})
	}
}
