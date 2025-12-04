# ClickHouse Native Serializer

`clickhouse-native` is a small Rust library focused on producing ClickHouse
`FORMAT Native` payloads exactly as the server would emit them. This makes it
possible to stream blocks straight into `INSERT … FORMAT Native`, save native
dumps to disk, or build custom ingestion tools without depending on the
official client. The project keeps feature parity with the upstream format,
including block headers, per-column compression envelopes, newer data types,
and reproducible checksums.

## Goals

- Emit byte streams that match ClickHouse output bit-for-bit (headers,
  per-column compression, type encodings).
- Allow callers to choose compression strategy per block (uncompressed, LZ4,
  ZSTD) or mix multiple codecs in a single stream.
- Provide strongly typed column builders so complex schemas can be assembled
  without dealing with raw bytes.
- Maintain a comprehensive, documented test-suite that pushes blocks directly
  into a ClickHouse server (see `tests/` for per-type and per-codec coverage).

## Common Usage

```rust
use clickhouse_native::{
    BlockBuilder, Compression, NativeWriter,
    column::GenericColumn,
    types::UInt8,
    StringColumn,
};

let mut ids = GenericColumn::new("id", UInt8);
ids.extend([1_u8, 2, 3]);
let mut names = StringColumn::new("name");
names.extend(["alpha", "beta", "gamma"]);

let mut builder = BlockBuilder::new();
builder.add_typed(ids)?;
builder.add_column(names.into())?;
let block = builder.build()?;

let mut writer = NativeWriter::with_compression(Vec::new(), Compression::Lz4);
writer.write_block(&block)?;
let native_payload = writer.into_inner()?; // send to ClickHouse as-is
```

The resulting `native_payload` can be POSTed to any HTTP endpoint using
`INSERT INTO table FORMAT Native` (see `tests/common` for a reusable client).

## Streaming From JSON

`NativeStreamBuilder` keeps memory usage flat by buffering rows column-wise and
flushing ClickHouse blocks once a configurable row budget is reached. It can
ingest typed rows through `RowSetter` or deserialize JSON objects directly:

```rust
use clickhouse_native::{
    Compression, NativeStreamBuilder, NativeWriter, Schema, StreamValue,
};
use serde_json::json;

let schema = Schema::from_type_strings(&[
    ("id", "UInt32"),
    ("name", "Nullable(String)"),
    ("score", "Float32"),
])?;
let writer = NativeWriter::with_compression(Vec::new(), Compression::Lz4);
let mut stream = NativeStreamBuilder::with_row_budget(writer, schema, 10_000)?;
stream.append_row(|row| {
    row.set("id", 1_u32)?;
    row.set("name", "alice")?;
    row.set("score", 9.5_f32)
})?;
stream.append_json(&json!({"id": 2, "name": null, "score": 7.25}))?;
let writer = stream.finish()?;
let native_payload = writer;
```

The `native_payload` produced above is a LZ4-compressed `FORMAT Native` stream
ready to be posted as-is to ClickHouse. See `tests/mixed/stream.rs` for end-to-end
examples that push builder-generated blocks into the test server.

`Schema` definitions may also include `LowCardinality(T)` columns. The streaming
builder keeps a per-column dictionary, reserves the same default/null slots as
ClickHouse, and automatically upgrades the index width (`UInt8` → `UInt16` → …)
when the dictionary grows. Only the inner types ClickHouse accepts (`UInt*`,
`Int*`, floats, `String`/`FixedString`, `Date`/`Date32`/`DateTime`, `UUID`,
`IPv4`, and `IPv6`) are allowed; attempting to wrap `DateTime64`, enums, or
other unsupported types yields `Error::InvalidValue`, mirroring the server’s
own parser. The resulting payloads are bit-for-bit identical to what the server
would emit for both nullable and non-nullable low-cardinality types (see
`tests/primitives/low_cardinality.rs`).

## Supported Types

Every primitive and composite type exposed by the library matches ClickHouse’s
wire format and semantic rules (integers, floats, dates, decimals, UUID/IP
types, strings, arrays, tuples, maps, enums, nested, low-cardinality columns,
etc.). Detailed notes covering per-type encodings, edge cases, and forbidden
combinations (for example `Array(Nested(T))` or `Nullable(Array(T))`) live in
[types.md](types.md).

## Running Tests

The repository includes extensive unit, integration, and compression tests that
exercise every type and codec against a live ClickHouse instance. The easiest
way to run everything (formatting, `cargo build`, `clippy`, unit tests, and the
full integration suite) is through:

```bash
./validate.sh
```

`validate.sh` sets the expected `RUSTFLAGS`, enforces formatting, and runs
`cargo test` with the `CLICKHOUSE_DSN` environment variable pointing to the
prepared server (see the script for defaults). Ensure the ClickHouse test
server from `validate.sh` is reachable before running the integration suite.

If you need to run individual tests you can still use `cargo test`, but the
integration modules expect `CLICKHOUSE_DSN` to be defined and accessible.
