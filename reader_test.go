package blobpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func writeRecords(t *testing.T, compressor Compressor, records []Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, compressor)
	for _, r := range records {
		if _, err := w.Write(r); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if _, err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func readAllRecords(t *testing.T, data []byte, decompressor Decompressor) []Record {
	t.Helper()
	r := NewReader(bytes.NewReader(data), decompressor)
	var result []Record
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		result = append(result, rec)
	}
	return result
}

func assertRecordsEqual(t *testing.T, got, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("record count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("[%d] Payload: got %q, want %q", i, got[i].Payload, want[i].Payload)
		}
	}
}

func TestRoundTripNoop(t *testing.T) {
	records := []Record{
		{Payload: []byte("alice")},
		{Payload: []byte("bob")},
		{Payload: []byte(`{"type":"click"}`)},
	}
	data := writeRecords(t, NoopCompressor{}, records)
	got := readAllRecords(t, data, NoopDecompressor{})
	assertRecordsEqual(t, got, records)
}

func TestRoundTripGzip(t *testing.T) {
	records := []Record{
		{Payload: []byte("some compressible data that repeats repeats repeats")},
		{Payload: []byte("another record")},
	}
	data := writeRecords(t, GzipCompressor{Level: -1}, records)
	got := readAllRecords(t, data, GzipDecompressor{})
	assertRecordsEqual(t, got, records)
}

func TestRoundTripEmptyPayload(t *testing.T) {
	records := []Record{
		{Payload: nil},
		{Payload: []byte{}},
	}
	data := writeRecords(t, NoopCompressor{}, records)
	got := readAllRecords(t, data, NoopDecompressor{})
	if len(got) != 2 {
		t.Fatalf("want 2 records, got %d", len(got))
	}
	for i, r := range got {
		if len(r.Payload) != 0 {
			t.Errorf("[%d] Payload should be empty, got %q", i, r.Payload)
		}
	}
}

func TestReadEOFOnEmpty(t *testing.T) {
	r := NewReader(bytes.NewReader(nil), NoopDecompressor{})
	_, err := r.Read()
	if !errors.Is(err, io.EOF) {
		t.Errorf("empty reader: got %v, want io.EOF", err)
	}
}

func TestReadCorruptCRC(t *testing.T) {
	data := writeRecords(t, NoopCompressor{}, []Record{
		{Payload: []byte("y")},
	})
	// flip the last byte (part of CRC32)
	data[len(data)-1] ^= 0xFF

	r := NewReader(bytes.NewReader(data), NoopDecompressor{})
	_, err := r.Read()
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("corrupt CRC: got %v, want ErrCorrupt", err)
	}
}

func TestReadTruncatedRecord(t *testing.T) {
	data := writeRecords(t, NoopCompressor{}, []Record{
		{Payload: []byte("hello world")},
	})
	truncated := data[:len(data)/2]

	r := NewReader(bytes.NewReader(truncated), NoopDecompressor{})
	_, err := r.Read()
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("truncated record: got %v, want ErrCorrupt", err)
	}
}

func TestReadMultipleRecords(t *testing.T) {
	records := []Record{
		{Payload: []byte("1")},
		{Payload: []byte("2")},
		{Payload: []byte("3")},
		{Payload: []byte("4")},
	}
	data := writeRecords(t, NoopCompressor{}, records)
	got := readAllRecords(t, data, NoopDecompressor{})
	assertRecordsEqual(t, got, records)
}

func TestReadLargePayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4*1024*1024) // 4 MB
	records := []Record{{Payload: payload}}
	data := writeRecords(t, NoopCompressor{}, records)
	got := readAllRecords(t, data, NoopDecompressor{})
	if !bytes.Equal(got[0].Payload, payload) {
		t.Error("large payload round-trip mismatch")
	}
}
