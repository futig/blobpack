package blobpack

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// ErrCorrupt is returned when a CRC32 mismatch is detected in a record.
var ErrCorrupt = errors.New("record-zipper: CRC32 mismatch, record is corrupt")

// Reader decodes records from the bundle binary format.
// It reads one record at a time from the underlying io.Reader.
//
// Reader is not safe for concurrent use.
type Reader struct {
	r            io.Reader
	decompressor Decompressor
}

// NewReader creates a Reader that reads encoded records from r and
// decompresses payloads using the given Decompressor.
func NewReader(r io.Reader, decompressor Decompressor) *Reader {
	return &Reader{r: r, decompressor: decompressor}
}

// ReadAll reads all records until EOF and returns them as a slice.
// Returns a partial result and the error if any record fails to decode.
func (r *Reader) ReadAll() ([]Record, error) {
	records := make([]Record, 0, 8)
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return records, err
		}
		records = append(records, rec)
	}
}

// Read decodes the next record from the stream.
// Returns (Record{}, io.EOF) when the stream is exhausted cleanly.
// Returns ErrCorrupt on CRC32 mismatch or a truncated record.
func (r *Reader) Read() (Record, error) {
	var lenBuf [fieldLenSize]byte
	_, err := io.ReadFull(r.r, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.EOF) {
				return Record{}, io.EOF
			}
			return Record{}, ErrCorrupt
		}
		return Record{}, fmt.Errorf("record-zipper: read length: %w", err)
	}
	length := byteOrder.Uint32(lenBuf[:])

	body := make([]byte, length)
	if _, err := io.ReadFull(r.r, body); err != nil {
		return Record{}, ErrCorrupt
	}

	if length < fieldCRC32Size {
		return Record{}, ErrCorrupt
	}
	compressedPayload := body[:length-fieldCRC32Size]
	storedCRC := byteOrder.Uint32(body[length-fieldCRC32Size:])
	if checksum(compressedPayload) != storedCRC {
		return Record{}, ErrCorrupt
	}

	var decompressed bytes.Buffer
	if err := r.decompressor.Decompress(&decompressed, bytes.NewReader(compressedPayload)); err != nil {
		return Record{}, fmt.Errorf("record-zipper: decompression failed: %w", err)
	}

	return Record{Payload: decompressed.Bytes()}, nil
}
