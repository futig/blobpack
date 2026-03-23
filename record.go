package blobpack

// Record is the unit of input to the Writer.
// Payload is the raw, uncompressed data for this record.
type Record struct {
	Payload []byte
}

// WriteStats is returned by Writer.Close and summarizes what was written.
type WriteStats struct {
	RecordCount  int
	BytesWritten int64
}

// RecordLocation describes where a record was written within the bundle.
// Offset is the byte offset of the record's first byte (the length field).
// Length is the total encoded size of the record in bytes.
type RecordLocation struct {
	Offset int64
	Length int64
}
