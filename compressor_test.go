package blobpack

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func compressDecompress(t *testing.T, c Compressor, d Decompressor, input []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	if err := c.Compress(&compressed, bytes.NewReader(input)); err != nil {
		t.Fatalf("Compress: %v", err)
	}
	var decompressed bytes.Buffer
	if err := d.Decompress(&decompressed, bytes.NewReader(compressed.Bytes())); err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	return decompressed.Bytes()
}

func TestNoopRoundTrip(t *testing.T) {
	input := []byte("hello, world")
	got := compressDecompress(t, NoopCompressor{}, NoopDecompressor{}, input)
	if !bytes.Equal(got, input) {
		t.Errorf("Noop round-trip: got %q, want %q", got, input)
	}
}

func TestGzipRoundTrip(t *testing.T) {
	input := []byte("some data that benefits from compression compression compression")
	got := compressDecompress(t, GzipCompressor{Level: -1}, GzipDecompressor{}, input)
	if !bytes.Equal(got, input) {
		t.Errorf("Gzip round-trip: got %q, want %q", got, input)
	}
}

func TestGzipDeterminism(t *testing.T) {
	input := []byte("deterministic output for same input")
	c := GzipCompressor{Level: gzip.DefaultCompression}

	var buf1, buf2 bytes.Buffer
	if err := c.Compress(&buf1, bytes.NewReader(input)); err != nil {
		t.Fatalf("first Compress: %v", err)
	}
	if err := c.Compress(&buf2, bytes.NewReader(input)); err != nil {
		t.Fatalf("second Compress: %v", err)
	}
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Error("GzipCompressor is not deterministic for same input")
	}
}

func TestGzipLevels(t *testing.T) {
	input := []byte("testing different compression levels")
	levels := []int{gzip.BestSpeed, gzip.BestCompression, gzip.DefaultCompression}
	for _, level := range levels {
		got := compressDecompress(t, GzipCompressor{Level: level}, GzipDecompressor{}, input)
		if !bytes.Equal(got, input) {
			t.Errorf("Gzip level %d: round-trip mismatch", level)
		}
	}
}

func TestGzipEmptyInput(t *testing.T) {
	got := compressDecompress(t, GzipCompressor{Level: -1}, GzipDecompressor{}, []byte{})
	if len(got) != 0 {
		t.Errorf("Gzip empty input: got %d bytes, want 0", len(got))
	}
}

func TestNoopEmptyInput(t *testing.T) {
	got := compressDecompress(t, NoopCompressor{}, NoopDecompressor{}, []byte{})
	if len(got) != 0 {
		t.Errorf("Noop empty input: got %d bytes, want 0", len(got))
	}
}
