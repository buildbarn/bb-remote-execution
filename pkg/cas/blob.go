package cas

import (
	"bytes"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Blob is an abstraction interface over a set amount of data to be
// uploaded. Calling any of its methods will consume the blob.
type Blob interface {
	ToReaderAt() buffer.ReadAtCloser
	ToByteSlice() ([]byte, error)
	Discard() error
}

type readerAtBlob struct {
	r         buffer.ReadAtCloser
	sizeBytes int64
}

func NewBlobFromReaderAt(r buffer.ReadAtCloser, sizeBytes int64) Blob {
	return &readerAtBlob{
		sizeBytes: sizeBytes,
		r:         r,
	}
}

func (b *readerAtBlob) ToReaderAt() buffer.ReadAtCloser {
	ret := b.r
	b.r = nil
	return ret
}

func (b *readerAtBlob) ToByteSlice() (data []byte, err error) {
	if b.r == nil {
		return nil, status.Error(codes.FailedPrecondition, "Blob has already been consumed")
	}

	defer func() {
		closeErr := b.r.Close()
		b.r = nil
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	data = make([]byte, b.sizeBytes)

	if n, readErr := b.r.ReadAt(data, 0); readErr != nil {
		if readErr == io.EOF {
			if n == len(data) {
				return data, nil
			}
			return nil, status.Errorf(codes.InvalidArgument, "Stream was %d bytes in size, while %d bytes were expected", n, b.sizeBytes)
		}
		return nil, readErr
	}

	return data, nil
}

func (b *readerAtBlob) Discard() error {
	if b.r == nil {
		return status.Error(codes.FailedPrecondition, "Blob has already been consumed")
	}
	err := b.r.Close()
	b.r = nil
	return err
}

type bytesliceBlob struct {
	data []byte
}

func NewBlobFromByteslice(data []byte) Blob {
	return &bytesliceBlob{data: data}
}

func (bytesliceBlob) Discard() error {
	return nil
}

func (b *bytesliceBlob) ToByteSlice() ([]byte, error) {
	return b.data, nil
}

func (b *bytesliceBlob) ToReaderAt() buffer.ReadAtCloser {
	return bytesliceReadAtCloser{
		Reader: bytes.NewReader(b.data),
	}
}

type bytesliceReadAtCloser struct {
	*bytes.Reader
}

func (bytesliceReadAtCloser) Close() error {
	return nil
}
