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
	offset     int64
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
// Write encodes r into the binary record format and writes it to the
// underlying writer in a single Write call.
// Returns a RecordLocation describing the offset and total byte length of the
// written record within the bundle.
func (w *Writer) Write(record Record) (RecordLocation, error) {
	if w.closed {
		return RecordLocation{}, ErrWriterClosed
	}

	w.buf.Reset()
	if err := w.compressor.Compress(&w.buf, bytes.NewReader(record.Payload)); err != nil {
		return RecordLocation{}, fmt.Errorf("record-zipper: compression failed: %w", err)
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
		return RecordLocation{}, fmt.Errorf("record-zipper: write failed: %w", err)
	}

	loc := RecordLocation{Offset: w.offset, Length: int64(n)}
	w.offset += int64(n)
	w.stats.BytesWritten += int64(n)
	w.stats.RecordCount++
	return loc, nil
}

// WriteAll writes each record in records and returns a slice of RecordLocations
// in the same order as the input. Returns a partial result and the error on
// the first failure.
func (w *Writer) WriteAll(records []Record) ([]RecordLocation, error) {
	locs := make([]RecordLocation, 0, len(records))
	for _, r := range records {
		loc, err := w.Write(r)
		if err != nil {
			return locs, err
		}
		locs = append(locs, loc)
	}
	return locs, nil
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
