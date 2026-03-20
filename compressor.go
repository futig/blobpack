package blobpack

import (
	"compress/gzip"
	"io"
)

// Compressor compresses data from src and writes it to dst.
// Implementations must be stateless and produce deterministic output.
type Compressor interface {
	Compress(dst io.Writer, src io.Reader) error
}

// Decompressor decompresses data from src and writes it to dst.
type Decompressor interface {
	Decompress(dst io.Writer, src io.Reader) error
}

// NoopCompressor passes data through without any compression.
type NoopCompressor struct{}

func (NoopCompressor) Compress(dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// NoopDecompressor passes data through without any decompression.
type NoopDecompressor struct{}

func (NoopDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// GzipCompressor compresses using gzip at the specified Level.
// Level must be one of the compress/gzip constants (e.g. gzip.BestSpeed,
// gzip.BestCompression, gzip.DefaultCompression).
// A new gzip.Writer is created per call to ensure deterministic output.
type GzipCompressor struct {
	Level int
}

func (g GzipCompressor) Compress(dst io.Writer, src io.Reader) error {
	gz, err := gzip.NewWriterLevel(dst, g.Level)
	if err != nil {
		return err
	}
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		return err
	}
	return gz.Close()
}

// GzipDecompressor decompresses gzip-encoded data.
type GzipDecompressor struct{}

func (GzipDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	gz, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer gz.Close()
	_, err = io.Copy(dst, gz)
	return err
}
