package blobpack

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"
)

// TestWriteSingleRecord verifies the binary layout field-by-field.
func TestWriteSingleRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})

	rec := Record{Payload: []byte("hello")}
	if _, err := w.Write(rec); err != nil {
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
		if _, err := w.Write(r); err != nil {
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

func TestRecordLocationIsolation(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, GzipCompressor{Level: gzip.BestCompression})

	records := []Record{
		{Payload: []byte("first")},
		{Payload: []byte("second")},
		{Payload: []byte("third")},
	}
	locs, err := w.WriteAll(records)
	if err != nil {
		t.Fatalf("WriteAll: %v", err)
	}

	bundle := buf.Bytes()

	for i, loc := range locs {
		slice := bundle[loc.Offset : loc.Offset+loc.Length]
		r := NewReader(bytes.NewReader(slice), GzipDecompressor{})
		rec, err := r.Read()
		if err != nil {
			t.Fatalf("[%d] Read from isolated slice: %v", i, err)
		}
		if !bytes.Equal(rec.Payload, records[i].Payload) {
			t.Errorf("[%d] Payload = %q, want %q", i, rec.Payload, records[i].Payload)
		}
	}
}

func TestWriteEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})
	if _, err := w.Write(Record{Payload: nil}); err != nil {
		t.Fatalf("Write with nil payload: %v", err)
	}
	if _, err := w.Write(Record{Payload: []byte{}}); err != nil {
		t.Fatalf("Write with empty payload: %v", err)
	}
}

func TestWriteAfterClose(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, NoopCompressor{})
	if _, err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write(Record{Payload: []byte("y")}); !errors.Is(err, ErrWriterClosed) {
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
	_, err := w.Write(Record{Payload: []byte("y")})
	if err == nil {
		t.Fatal("expected error from underlying writer, got nil")
	}
}

func TestWriterWithGzip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, GzipCompressor{Level: -1})
	if _, err := w.Write(Record{Payload: []byte("compressed data")}); err != nil {
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
