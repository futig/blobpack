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
