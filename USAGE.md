# Usage Examples

This document covers advanced usage patterns for both Rust and Python.

## Table of Contents

- [Rust Examples](#rust-examples)
  - [Write a Zstd-compressed file](#write-a-seekable-zstd-rowbinarywithnames-and-types-file)
  - [Read and batch for HTTP insert](#read-a-seekable-zstd-file-and-post-in-batches)
  - [Dynamic values](#dynamic-values)
  - [Nested columns](#writing-nested-columns)
  - [Multi-threaded production](#combine-per-thread-rowbinary-chunks-into-one-zstd-file)
- [Python Examples](#python-examples)
  - [Batch inserts to ClickHouse](#batch-inserts-to-clickhouse)
  - [Reading from files](#reading-from-files)
  - [Complex types](#complex-types)
  - [Generator-based writing](#memory-efficient-writing-with-generators)

---

## Rust Examples

For convenience, `RowBinaryFileReader` and `RowBinaryFileWriter` are type
aliases for buffered file-backed readers/writers (`BufReader<File>` and
`BufWriter<File>`). `RowBinaryValueWriter::new_buffered` wraps any writer in
`BufWriter` when you want buffering for small row payloads.

### Write a seekable Zstd RowBinaryWithNamesAndTypes file

```rust
use std::fs::File;
use std::io::BufWriter;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryWriter, RowBinaryValueWriter, Schema, Value,
};

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;
let rows = vec![
    vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
    vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
];

let file = File::create("data.rowbinary.zst")?;
let mut writer =
    RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNamesAndTypes)?;
writer.write_header(&schema)?;
for row in &rows {
    let mut row_writer =
        RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema.clone());
    row_writer.write_header()?;
    row_writer.write_row(row)?;
    writer.write_row_bytes(&row_writer.into_inner())?;
}
writer.finish()?;
```

### Read a seekable Zstd file and post in batches

The `RowBinaryWithNamesAndTypes` header is required for each INSERT. For best
performance, stream row bytes directly into the batch writer and only rebuild
the payload when you hit the batch size.

```rust
use std::fs::File;
use std::io::BufReader;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryReader, RowBinaryValueWriter, Schema,
};

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;
let file = File::open("data.rowbinary.zst")?;
let mut reader = RowBinaryReader::new(
    BufReader::new(file),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    None,
)?;
// The constructor parses the header and initializes the schema.
// For large files, consider RowBinaryReader::new_with_stride(..., 1024)
// to keep the in-memory row index sparse.

let mut out = RowBinaryValueWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema.clone(),
);
out.write_header()?;

let mut count = 0usize;
loop {
    let Some(row) = reader.current_row()? else {
        break;
    };
    out.write_row_bytes(row)?;
    count += 1;
    if count == 100_000 {
        let mut payload = out.take_inner();
        // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
        payload.clear();
        out.reset(payload);
        count = 0;
    }
    if reader.seek_relative(1).is_err() {
        break;
    }
}

if count > 0 {
    let payload = out.into_inner();
    // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
}

// Seek to a specific row if you need to retry from a prior position.
reader.seek_row(50_000)?;
```

For tighter control over allocations, keep a reusable `Vec<u8>` per batch,
`clear()` it after sending, and pass it into each new `RowBinaryValueWriter`.
If your HTTP client supports streaming request bodies, you can write directly
into the request stream instead of buffering the entire batch.

### Dynamic values

`Dynamic` values encode the concrete type before each value using ClickHouse's
binary type encoding. Use `Value::Dynamic` with an explicit `TypeDesc`, or
`Value::DynamicNull` to emit `Nothing`.

Note: when Dynamic values are produced by ClickHouse SQL casts, `Nested` is
encoded as `Array(Tuple(...))`, so the decoded `TypeDesc` will be that array
form rather than `Nested`.

```rust
use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, TypeDesc, Value};

let schema = Schema::from_type_strings(&[("value", "Dynamic")])?;
let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_header()?;
writer.write_rows(&[
    vec![Value::Dynamic {
        ty: Box::new(TypeDesc::UInt8),
        value: Box::new(Value::UInt8(7)),
    }],
    vec![Value::Dynamic {
        ty: Box::new(TypeDesc::String),
        value: Box::new(Value::String(b"alpha".to_vec())),
    }],
    vec![Value::DynamicNull],
])?;
let payload = writer.into_inner();
// INSERT INTO table FORMAT RowBinary
```

### Writing Nested columns

ClickHouse expands `Nested` columns into separate `Array(T)` columns on write
(`n.a`, `n.b`, ...). The writer handles this for you: supply a `Nested` schema
and pass `Value::Array(Vec<Value::Tuple>)` for the column value. The writer
will emit `n.a`, `n.b` payloads in the expected order. This conversion buffers
the nested values to transpose rows into per-field arrays.

```rust
use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, Value};

let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")])?;
let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_header()?;
writer.write_row(&[Value::Array(vec![
    Value::Tuple(vec![Value::UInt8(7), Value::String(b"alpha".to_vec())]),
    Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec())]),
])])?;
let payload = writer.into_inner();
// INSERT INTO table FORMAT RowBinary
```

### Combine per-thread RowBinary chunks into one ZSTD file

Workers can emit **plain RowBinary** (no header) and a single aggregator writes
one `RowBinaryWithNamesAndTypes` header before appending worker chunks.

```rust
use std::fs::File;
use std::io::Write;
use std::thread;

use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, Value};
use zstd::stream::Encoder;

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;

let handle = |rows: Vec<Vec<Value>>| {
    thread::spawn(move || {
        let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
        writer.write_header()?;
        writer.write_rows(&rows)?;
        Ok::<_, clickhouse_rowbinary::Error>(writer.into_inner())
    })
};

let h1 = handle(vec![vec![Value::UInt8(1), Value::String(b"a".to_vec())]]);
let h2 = handle(vec![vec![Value::UInt8(2), Value::String(b"b".to_vec())]]);
let chunk1 = h1.join().unwrap()?;
let chunk2 = h2.join().unwrap()?;

let file = File::create("combined.rowbinary.zst")?;
let mut encoder = Encoder::new(file, 0)?;

// Write one header, then append raw row bytes.
let mut header_writer = RowBinaryValueWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema,
);
header_writer.write_header()?;
encoder.write_all(&header_writer.into_inner())?;
encoder.write_all(&chunk1)?;
encoder.write_all(&chunk2)?;
encoder.finish()?;
```

---

## Python Examples

### Batch inserts to ClickHouse

Efficiently insert large datasets by batching rows and using the HTTP interface:

```python
import httpx
from clickhouse_rowbinary import Schema, RowBinaryWriter, Format

schema = Schema.from_clickhouse([
    ("id", "UInt64"),
    ("name", "String"),
    ("timestamp", "DateTime"),
])

def insert_batch(rows: list, client: httpx.Client, table: str):
    """Insert a batch of rows to ClickHouse."""
    writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
    writer.write_header()
    writer.write_rows(rows)
    data = writer.take()

    response = client.post(
        "http://localhost:8123/",
        params={"query": f"INSERT INTO {table} FORMAT RowBinaryWithNamesAndTypes"},
        content=data,
    )
    response.raise_for_status()

# Usage with batching
BATCH_SIZE = 10_000
batch = []
client = httpx.Client()

for row in generate_rows():
    batch.append(row)
    if len(batch) >= BATCH_SIZE:
        insert_batch(batch, client, "my_table")
        batch.clear()

if batch:
    insert_batch(batch, client, "my_table")
```

### Reading from files

Read RowBinary data directly from files for efficient streaming:

```python
from clickhouse_rowbinary import Schema, RowBinaryReader

schema = Schema.from_clickhouse([
    ("id", "UInt32"),
    ("name", "String"),
    ("value", "Float64"),
])

# Stream from file (memory-efficient for large files)
reader = RowBinaryReader.from_file("data.bin", schema)
for row in reader:
    process_row(row)

# Or read all at once (releases GIL, good for parallel processing)
reader = RowBinaryReader.from_file("data.bin", schema)
rows = reader.read_all()
# Now process rows in parallel if needed
```

### Complex types

Working with nested, nullable, and composite types:

```python
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from ipaddress import IPv4Address
from clickhouse_rowbinary import Schema, RowBinaryWriter, RowBinaryReader

schema = Schema.from_clickhouse([
    # Nullable types
    ("optional_value", "Nullable(Int32)"),

    # Arrays
    ("tags", "Array(String)"),

    # Maps
    ("metadata", "Map(String, Int64)"),

    # Nested tuples
    ("point", "Tuple(Float64, Float64)"),

    # Date/time types
    ("created_at", "DateTime64(3)"),
    ("birth_date", "Date"),

    # Other scalar types
    ("id", "UUID"),
    ("ip", "IPv4"),
    ("amount", "Decimal64(2)"),
])

writer = RowBinaryWriter(schema)
writer.write_row({
    "optional_value": None,  # Nullable - can be None
    "tags": [b"python", b"clickhouse", b"fast"],
    "metadata": {b"views": 1000, b"likes": 42},
    "point": (37.7749, -122.4194),
    "created_at": datetime.now(),
    "birth_date": date(1990, 5, 15),
    "id": UUID("550e8400-e29b-41d4-a716-446655440000"),
    "ip": IPv4Address("192.168.1.1"),
    "amount": Decimal("99.99"),
})

data = writer.take()

# Read back with string mode for UTF-8 strings
reader = RowBinaryReader(data, schema, string_mode="str")
row = reader.read_row()
print(row["tags"])  # ['python', 'clickhouse', 'fast']
```

### Memory-efficient writing with generators

Process large datasets without loading everything into memory:

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter

schema = Schema.from_clickhouse([
    ("id", "UInt64"),
    ("data", "String"),
])

def generate_rows(count: int):
    """Generate rows on-demand."""
    for i in range(count):
        yield {"id": i, "data": f"row_{i}".encode()}

# Write 1 million rows without storing them all in memory
writer = RowBinaryWriter(schema)
writer.write_rows(generate_rows(1_000_000))
data = writer.take()

print(f"Wrote {writer.rows_written} rows, {len(data)} bytes")
```

### Enum handling

Enums are written as strings (variant names) and read back as strings:

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter, RowBinaryReader

schema = Schema.from_clickhouse([
    ("status", "Enum8('pending' = 0, 'active' = 1, 'completed' = 2)"),
])

writer = RowBinaryWriter(schema)
writer.write_row({"status": "active"})
writer.write_row({"status": "completed"})
data = writer.take()

reader = RowBinaryReader(data, schema)
for row in reader:
    print(row["status"])  # "active", "completed"
```

### Working with LowCardinality

LowCardinality is transparent in the Python API - use the same types:

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter

# LowCardinality wraps the inner type
schema = Schema.from_clickhouse([
    ("country", "LowCardinality(String)"),
    ("status", "LowCardinality(Nullable(String))"),
])

writer = RowBinaryWriter(schema)
writer.write_row({"country": b"US", "status": b"active"})
writer.write_row({"country": b"UK", "status": None})  # Nullable allows None
```

### Error recovery pattern

Handle errors gracefully when processing large datasets:

```python
from clickhouse_rowbinary import (
    Schema, RowBinaryWriter, RowBinaryReader,
    ValidationError, EncodingError, DecodingError
)

schema = Schema.from_clickhouse([("id", "UInt32"), ("value", "Float64")])

def safe_write_rows(rows, schema):
    """Write rows, skipping invalid ones."""
    writer = RowBinaryWriter(schema)
    skipped = 0

    for row in rows:
        try:
            writer.write_row(row)
        except (ValidationError, EncodingError) as e:
            print(f"Skipping invalid row: {e}")
            skipped += 1
            continue

    print(f"Wrote {writer.rows_written} rows, skipped {skipped}")
    return writer.take()

def safe_read_rows(data, schema):
    """Read rows, handling corrupted data."""
    try:
        reader = RowBinaryReader(data, schema)
        return reader.read_all()
    except DecodingError as e:
        print(f"Data corrupted: {e}")
        return []
```
