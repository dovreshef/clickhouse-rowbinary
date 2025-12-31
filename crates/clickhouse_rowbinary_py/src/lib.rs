//! Python bindings for the `clickhouse_rowbinary` library.

// PyO3 0.27 deprecates some APIs that we use, allow until migration to new APIs
#![allow(deprecated)]

use pyo3::prelude::*;

mod convert;
mod errors;
mod format;
mod reader;
mod row;
mod schema;
mod writer;

/// Python module exposing RowBinary functionality.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Format enum
    m.add_class::<format::Format>()?;

    // Schema and Column
    m.add_class::<schema::Schema>()?;
    m.add_class::<schema::Column>()?;

    // Row
    m.add_class::<row::Row>()?;

    // Writer
    m.add_class::<writer::RowBinaryWriter>()?;

    // Reader
    m.add_class::<reader::RowBinaryReader>()?;

    // Exceptions
    m.add(
        "ClickHouseRowBinaryError",
        m.py().get_type::<errors::ClickHouseRowBinaryError>(),
    )?;
    m.add("SchemaError", m.py().get_type::<errors::SchemaError>())?;
    m.add(
        "ValidationError",
        m.py().get_type::<errors::ValidationError>(),
    )?;
    m.add("EncodingError", m.py().get_type::<errors::EncodingError>())?;
    m.add("DecodingError", m.py().get_type::<errors::DecodingError>())?;

    Ok(())
}
