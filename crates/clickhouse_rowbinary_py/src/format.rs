//! RowBinary format enum wrapper.

use pyo3::prelude::*;

use clickhouse_rowbinary::RowBinaryFormat as RustFormat;

/// RowBinary format variants.
///
/// Attributes:
///     RowBinary: Plain RowBinary format (no header).
///     RowBinaryWithNames: RowBinary with column names header.
///     RowBinaryWithNamesAndTypes: RowBinary with column names and types
/// header.
#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// Plain RowBinary (no names, no types).
    RowBinary,
    /// RowBinary with column names.
    RowBinaryWithNames,
    /// RowBinary with column names and types.
    RowBinaryWithNamesAndTypes,
}

#[pymethods]
impl Format {
    /// Returns the ClickHouse format name as a string.
    ///
    /// Returns:
    ///     str: The format name (e.g., "RowBinary", "RowBinaryWithNames").
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __str__(&self) -> &'static str {
        match self {
            Format::RowBinary => "RowBinary",
            Format::RowBinaryWithNames => "RowBinaryWithNames",
            Format::RowBinaryWithNamesAndTypes => "RowBinaryWithNamesAndTypes",
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        format!("Format.{}", self.__str__())
    }
}

impl From<Format> for RustFormat {
    fn from(format: Format) -> Self {
        match format {
            Format::RowBinary => RustFormat::RowBinary,
            Format::RowBinaryWithNames => RustFormat::RowBinaryWithNames,
            Format::RowBinaryWithNamesAndTypes => RustFormat::RowBinaryWithNamesAndTypes,
        }
    }
}

impl From<RustFormat> for Format {
    fn from(format: RustFormat) -> Self {
        match format {
            RustFormat::RowBinary => Format::RowBinary,
            RustFormat::RowBinaryWithNames => Format::RowBinaryWithNames,
            RustFormat::RowBinaryWithNamesAndTypes => Format::RowBinaryWithNamesAndTypes,
        }
    }
}
