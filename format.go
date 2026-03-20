package blobpack

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	fieldLenSize   = 4 // uint32: total record size excluding itself
	fieldCRC32Size = 4 // uint32: CRC32 checksum
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// byteOrder is the byte order used for all multi-byte integers in the format.
var byteOrder = binary.BigEndian

// checksum computes CRC32(IEEE) over the given bytes.
func checksum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}
