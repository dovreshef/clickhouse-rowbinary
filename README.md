# clickhouse-rowbinary

Fast, streaming encoder/decoder for ClickHouse [RowBinary](https://clickhouse.com/docs/en/interfaces/formats#rowbinary) format.

Available as both a **Rust crate** and **Python package** (with Rust-powered performance).

## Features

- **Full type support**: All ClickHouse types including Decimal, DateTime64, UUID, IPv4/IPv6, Nullable, Array, Map, Tuple, LowCardinality, Nested, JSON, and Dynamic
- **Three format variants**: `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes`
- **Streaming APIs**: Read/write rows incrementally without loading entire datasets into memory
- **Schema validation**: Strict type checking with clear error messages
- **Zero-copy where possible**: Efficient memory usage for high-throughput scenarios

## Installation

### Python

```bash
pip install clickhouse-rowbinary
```

### Rust

```toml
[dependencies]
clickhouse_rowbinary = "0.1"
```

## Quick Start

### Python

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter, RowBinaryReader

# Define schema
schema = Schema.from_clickhouse([
    ("id", "UInt32"),
    ("name", "String"),
    ("score", "Float64"),
])

# Write rows
writer = RowBinaryWriter(schema)
writer.write_row({"id": 1, "name": b"Alice", "score": 95.5})
writer.write_row({"id": 2, "name": b"Bob", "score": 87.0})
data = writer.take()

# Read rows back
reader = RowBinaryReader(data, schema)
for row in reader:
    print(f"{row['name'].decode()}: {row['score']}")
```

### Rust

```rust
use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, Value};

let schema = Schema::from_type_strings(&[
    ("id", "UInt32"),
    ("name", "String"),
    ("score", "Float64"),
])?;

let mut writer = RowBinaryValueWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinary,
    schema.clone(),
);
writer.write_row(&[
    Value::UInt32(1),
    Value::String(b"Alice".to_vec()),
    Value::Float64(95.5),
])?;

let payload = writer.into_inner();
```

## Supported Types

| ClickHouse Type | Python Type | Rust Type |
|-----------------|-------------|-----------|
| `UInt8`..`UInt256` | `int` | `Value::UInt8`..`Value::UInt256` |
| `Int8`..`Int256` | `int` | `Value::Int8`..`Value::Int256` |
| `Float32`, `Float64` | `float` | `Value::Float32`, `Value::Float64` |
| `Bool` | `bool` | `Value::Bool` |
| `String` | `bytes` or `str` | `Value::String` |
| `FixedString(N)` | `bytes` | `Value::FixedString` |
| `Date`, `Date32` | `datetime.date` | `Value::Date`, `Value::Date32` |
| `DateTime` | `datetime.datetime` | `Value::DateTime` |
| `DateTime64(p)` | `datetime.datetime` | `Value::DateTime64` |
| `UUID` | `uuid.UUID` | `Value::Uuid` |
| `IPv4`, `IPv6` | `ipaddress.*` | `Value::IPv4`, `Value::IPv6` |
| `Decimal*` | `decimal.Decimal` | `Value::Decimal*` |
| `Enum8`, `Enum16` | `str` | `Value::Enum8`, `Value::Enum16` |
| `Nullable(T)` | `T \| None` | `Value::Nullable` |
| `Array(T)` | `list` | `Value::Array` |
| `Map(K, V)` | `dict` | `Value::Map` |
| `Tuple(...)` | `tuple` | `Value::Tuple` |
| `LowCardinality(T)` | same as `T` | same as `T` |
| `JSON` | `dict` | `Value::Json` |
| `Dynamic` | varies | `Value::Dynamic` |

## Format Variants

- **`RowBinary`**: Raw row data only (most compact, requires schema)
- **`RowBinaryWithNames`**: Includes column names header
- **`RowBinaryWithNamesAndTypes`**: Self-describing with names and types

```python
from clickhouse_rowbinary import Format

# For self-describing data
writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
writer.write_header()
writer.write_rows(rows)
```

## Documentation

- **Python**: See the [Python package documentation](python/README.md) for detailed Python API reference
- **Rust**: See [USAGE.md](USAGE.md) for advanced Rust patterns (streaming, batching, Zstd compression)
- **API Reference**: [docs.rs/clickhouse_rowbinary](https://docs.rs/clickhouse_rowbinary)

## Error Handling

Both implementations provide specific exception/error types:

| Python Exception | Rust Error | When Raised |
|------------------|------------|-------------|
| `SchemaError` | `Error::UnsupportedType` | Invalid type string |
| `ValidationError` | `Error::TypeMismatch` | Data doesn't match schema |
| `EncodingError` | `Error::Overflow` | Value out of range |
| `DecodingError` | `Error::Io` | Corrupted/truncated data |

## Running Tests

```bash
# Rust tests
cargo test --lib

# Python tests
uv run pytest tests/python

# Full validation (requires ClickHouse)
./validate.sh
```

## License

MIT OR Apache-2.0
