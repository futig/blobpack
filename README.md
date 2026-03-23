# blobpack

A minimal Go library for writing and reading streams of binary records with optional compression and CRC32 integrity checks.

## Binary format

Each record is encoded as:

```
[uint32 length][compressed payload][uint32 crc32]
```

- `length` — byte count of `compressed payload + crc32` (big-endian)
- `crc32` — IEEE CRC32 over the compressed payload bytes

## Installation

```sh
go get github.com/futig/blobpack
```

## Usage

### Writing records

`Write` returns a `RecordLocation` with the byte offset and total length of the written record within the bundle. `WriteAll` returns a slice of locations in the same order as the input records.

```go
package main

import (
    "compress/gzip"
    "fmt"
    "os"

    "github.com/futig/blobpack"
)

func main() {
    f, _ := os.Create("data.blob")
    defer f.Close()

    w := blobpack.NewWriter(f, blobpack.GzipCompressor{Level: gzip.BestSpeed})

    records := []blobpack.Record{
        {Payload: []byte("hello")},
        {Payload: []byte("world")},
    }

    locs, err := w.WriteAll(records)
    if err != nil {
        panic(err)
    }
    for i, loc := range locs {
        fmt.Printf("record %d: offset=%d length=%d\n", i, loc.Offset, loc.Length)
    }

    stats, _ := w.Close()
    // stats.RecordCount, stats.BytesWritten
}
```

To write a single record:

```go
loc, err := w.Write(blobpack.Record{Payload: []byte("hello")})
// loc.Offset — byte offset of the record in the bundle
// loc.Length — total encoded size in bytes
```

### Reading records

```go
package main

import (
    "fmt"
    "os"

    "github.com/futig/blobpack"
)

func main() {
    f, _ := os.Open("data.blob")
    defer f.Close()

    r := blobpack.NewReader(f, blobpack.GzipDecompressor{})

    records, err := r.ReadAll()
    if err != nil {
        panic(err)
    }

    for _, rec := range records {
        fmt.Println(string(rec.Payload))
    }
}
```

### Reading one record at a time

```go
r := blobpack.NewReader(f, blobpack.GzipDecompressor{})

for {
    rec, err := r.Read()
    if err == io.EOF {
        break
    }
    if err != nil {
        // blobpack.ErrCorrupt on CRC mismatch or truncated data
        panic(err)
    }
    fmt.Println(string(rec.Payload))
}
```

### Without compression

```go
w := blobpack.NewWriter(f, blobpack.NoopCompressor{})
r := blobpack.NewReader(f, blobpack.NoopDecompressor{})
```

### Custom compressor

Implement the `Compressor` / `Decompressor` interfaces:

```go
type Compressor interface {
    Compress(dst io.Writer, src io.Reader) error
}

type Decompressor interface {
    Decompress(dst io.Writer, src io.Reader) error
}
```

## Built-in compressors

| Type                  | Description                                              |
|-----------------------|----------------------------------------------------------|
| `NoopCompressor`      | No compression (pass-through)                            |
| `NoopDecompressor`    | No decompression (pass-through)                          |
| `GzipCompressor`      | gzip with configurable `Level`                           |
| `GzipDecompressor`    | gzip decompression with configurable `MaxBytes` limit    |

`GzipDecompressor.MaxBytes` caps the size of the decompressed output to guard against decompression bombs. Defaults to 256 MiB when zero.

```go
// default limit (256 MiB)
r := blobpack.NewReader(f, blobpack.GzipDecompressor{})

// custom limit
r := blobpack.NewReader(f, blobpack.GzipDecompressor{MaxBytes: 64 << 20}) // 64 MiB
```

## Notes

- `Writer` and `Reader` are not safe for concurrent use.
- `Writer.Close` does **not** close the underlying `io.Writer`.
- A corrupt or truncated record returns `blobpack.ErrCorrupt`.
- `RecordLocation.Offset` and `RecordLocation.Length` refer to the full encoded record (length field + compressed payload + CRC32), so `bundle[loc.Offset : loc.Offset+loc.Length]` is a valid self-contained record that can be passed directly to `NewReader`.
