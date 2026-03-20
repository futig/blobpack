package blobpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestWriteSingleRecord verifies the binary layout field-by-field.
func TestWriteSingleRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})

	rec := Record{Payload: []byte("hello")}
	if err := w.Write(rec); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data := buf.Bytes()

	// length field
	length := byteOrder.Uint32(data[0:4])

	// body = [payload="hello"]
	expectedBodyLen := len("hello")
	expectedLength := uint32(expectedBodyLen + fieldCRC32Size)
	if length != expectedLength {
		t.Errorf("length = %d, want %d", length, expectedLength)
	}

	// payload (noop = raw)
	if string(data[4:9]) != "hello" {
		t.Errorf("payload = %q, want %q", data[4:9], "hello")
	}

	// CRC32 over compressed payload
	body := data[4 : 4+expectedBodyLen]
	wantCRC := checksum(body)
	gotCRC := byteOrder.Uint32(data[4+expectedBodyLen:])
	if gotCRC != wantCRC {
		t.Errorf("CRC32 = %d, want %d", gotCRC, wantCRC)
	}

	// total bytes
	want := fieldLenSize + int(expectedLength)
	if len(data) != want {
		t.Errorf("total bytes = %d, want %d", len(data), want)
	}
}

func TestWriteMultipleRecords(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})

	records := []Record{
		{Payload: []byte("first")},
		{Payload: []byte("second")},
		{Payload: []byte("third")},
	}
	for _, r := range records {
		if err := w.Write(r); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	stats, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3", stats.RecordCount)
	}
	if stats.BytesWritten != int64(buf.Len()) {
		t.Errorf("BytesWritten = %d, want %d", stats.BytesWritten, buf.Len())
	}
}

func TestWriteEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})
	if err := w.Write(Record{Payload: nil}); err != nil {
		t.Fatalf("Write with nil payload: %v", err)
	}
	if err := w.Write(Record{Payload: []byte{}}); err != nil {
		t.Fatalf("Write with empty payload: %v", err)
	}
}

func TestWriteAfterClose(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})
	if _, err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Write(Record{Payload: []byte("y")}); !errors.Is(err, ErrWriterClosed) {
		t.Errorf("Write after Close: got %v, want ErrWriterClosed", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})
	if _, err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if _, err := w.Close(); !errors.Is(err, ErrWriterClosed) {
		t.Errorf("second Close: got %v, want ErrWriterClosed", err)
	}
}

func TestWriteErrorPropagation(t *testing.T) {
	w := NewWriter(errorWriter{}, NoopCompressor{})
	err := w.Write(Record{Payload: []byte("y")})
	if err == nil {
		t.Fatal("expected error from underlying writer, got nil")
	}
}

func TestWriterWithGzip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, GzipCompressor{Level: -1})
	if err := w.Write(Record{Payload: []byte("compressed data")}); err != nil {
		t.Fatalf("Write with gzip: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected bytes written with gzip")
	}
}

// errorWriter always returns an error on Write.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}
