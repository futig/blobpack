package blobpack

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// ErrWriterClosed is returned when Write or Close is called on a closed Writer.
var ErrWriterClosed = errors.New("record-zipper: writer is already closed")

// Writer encodes records into the bundle binary format and writes them
// to an underlying io.Writer. Only one record is held in memory at a time.
//
// Writer does not close or flush the underlying io.Writer on Close.
// Writer is not safe for concurrent use.
type Writer struct {
	w          io.Writer
	compressor Compressor
	stats      WriteStats
	buf        bytes.Buffer // reused compression scratch buffer
	closed     bool
}

// NewWriter creates a Writer that encodes records using the given Compressor
// and writes encoded bytes to w.
func NewWriter(w io.Writer, compressor Compressor) *Writer {
	return &Writer{
		w:          w,
		compressor: compressor,
	}
}

// Write encodes r into the binary record format and writes it to the
// underlying writer in a single Write call.
//
// Record format:
//
//	[uint32 length][compressed payload][uint32 crc32]
//
// length covers everything after itself.
// crc32 covers the compressed payload bytes.
func (w *Writer) Write(record Record) error {
	if w.closed {
		return ErrWriterClosed
	}

	w.buf.Reset()
	if err := w.compressor.Compress(&w.buf, bytes.NewReader(record.Payload)); err != nil {
		return fmt.Errorf("record-zipper: compression failed: %w", err)
	}
	compressedPayload := w.buf.Bytes()

	crc := checksum(compressedPayload)

	length := uint32(len(compressedPayload) + fieldCRC32Size)
	assembled := make([]byte, fieldLenSize+int(length))
	byteOrder.PutUint32(assembled[0:], length)
	copy(assembled[fieldLenSize:], compressedPayload)
	byteOrder.PutUint32(assembled[fieldLenSize+len(compressedPayload):], crc)

	n, err := w.w.Write(assembled)
	if err != nil {
		return fmt.Errorf("record-zipper: write failed: %w", err)
	}

	w.stats.BytesWritten += int64(n)
	w.stats.RecordCount++
	return nil
}

// WriteAll writes each record in records. Returns on the first error.
func (w *Writer) WriteAll(records []Record) error {
	for _, r := range records {
		if err := w.Write(r); err != nil {
			return err
		}
	}
	return nil
}

// Close marks the writer as closed and returns accumulated write statistics.
// It does NOT close the underlying io.Writer.
// Calling Close more than once returns ErrWriterClosed.
func (w *Writer) Close() (WriteStats, error) {
	if w.closed {
		return WriteStats{}, ErrWriterClosed
	}
	w.closed = true
	return w.stats, nil
}
