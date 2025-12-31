//! Schema and Column wrappers for Python.

use std::sync::Arc;

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use clickhouse_rowbinary::{Field, Schema as RustSchema, parse_type_desc};

use crate::errors::{SchemaError, to_py_err};

/// A single column definition in a schema.
///
/// Attributes:
///     name (str): The column name.
///     type_str (str): The ClickHouse type as a string.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Column {
    pub(crate) field: Field,
}

#[pymethods]
impl Column {
    /// Creates a new Column.
    ///
    /// Args:
    ///     name: The column name.
    ///     type_str: The ClickHouse type string (e.g., "UInt32",
    /// "Nullable(String)").
    ///
    /// Returns:
    ///     Column: A new column definition.
    ///
    /// Raises:
    ///     SchemaError: If the type string is invalid.
    #[new]
    fn new(name: String, type_str: &str) -> PyResult<Self> {
        let ty = parse_type_desc(type_str).map_err(to_py_err)?;
        Ok(Self {
            field: Field { name, ty },
        })
    }

    /// The column name.
    #[getter]
    fn name(&self) -> &str {
        &self.field.name
    }

    /// The ClickHouse type as a string.
    #[getter]
    fn type_str(&self) -> String {
        self.field.ty.type_name()
    }

    fn __repr__(&self) -> String {
        format!(
            "Column('{}', '{}')",
            self.field.name,
            self.field.ty.type_name()
        )
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.field == other.field
    }
}

/// A schema defining the structure of RowBinary data.
///
/// A schema contains ordered column definitions that specify the names
/// and types of data to be read or written in RowBinary format.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Schema {
    pub(crate) inner: Arc<RustSchema>,
}

#[pymethods]
impl Schema {
    /// Creates a schema from a list of (name, type) tuples.
    ///
    /// This is the recommended way to create a schema from ClickHouse
    /// column definitions.
    ///
    /// Args:
    ///     columns: A list of (name, type_str) tuples defining the columns.
    ///
    /// Returns:
    ///     Schema: A new schema.
    ///
    /// Raises:
    ///     SchemaError: If any type string is invalid.
    ///
    /// Example:
    ///     >>> schema = Schema.from_clickhouse([
    ///     ...     ("id", "UInt32"),
    ///     ...     ("name", "String"),
    ///     ...     ("age", "Nullable(UInt8)")
    ///     ... ])
    #[staticmethod]
    #[pyo3(signature = (columns))]
    fn from_clickhouse(columns: &Bound<'_, PyList>) -> PyResult<Self> {
        let mut fields = Vec::with_capacity(columns.len());

        for item in columns.iter() {
            let tuple = item
                .downcast::<PyTuple>()
                .map_err(|_| SchemaError::new_err("Expected list of (name, type) tuples"))?;

            if tuple.len() != 2 {
                return Err(SchemaError::new_err(
                    "Each column must be a (name, type) tuple",
                ));
            }

            let name: String = tuple.get_item(0)?.extract()?;
            let type_str: String = tuple.get_item(1)?.extract()?;

            let ty = parse_type_desc(&type_str).map_err(to_py_err)?;
            fields.push(Field { name, ty });
        }

        Ok(Self {
            inner: Arc::new(RustSchema::new(fields)),
        })
    }

    /// Creates a schema from Column objects.
    ///
    /// Args:
    ///     columns: A list of Column objects.
    ///
    /// Returns:
    ///     Schema: A new schema.
    #[new]
    #[pyo3(signature = (columns))]
    fn new(columns: Vec<Column>) -> Self {
        let fields = columns.into_iter().map(|c| c.field).collect();
        Self {
            inner: Arc::new(RustSchema::new(fields)),
        }
    }

    /// Returns the columns in the schema.
    ///
    /// Returns:
    ///     list[Column]: The column definitions.
    #[getter]
    fn columns(&self) -> Vec<Column> {
        self.inner
            .fields()
            .iter()
            .map(|f| Column { field: f.clone() })
            .collect()
    }

    /// Returns the column names.
    ///
    /// Returns:
    ///     list[str]: The column names in order.
    #[getter]
    fn names(&self) -> Vec<&str> {
        self.inner
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect()
    }

    /// Returns the number of columns.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Returns True if the schema has no columns.
    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }

    /// Gets a column by index or name.
    ///
    /// Args:
    ///     key: Either an integer index or a string column name.
    ///
    /// Returns:
    ///     Column: The column at the given index or with the given name.
    ///
    /// Raises:
    ///     IndexError: If the index is out of range.
    ///     KeyError: If the column name is not found.
    fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<Column> {
        if let Ok(index) = key.extract::<isize>() {
            let len = self.inner.len() as isize;
            let idx = if index < 0 { len + index } else { index };

            if idx < 0 || idx >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err(
                    "schema index out of range",
                ));
            }

            Ok(Column {
                field: self.inner.fields()[idx as usize].clone(),
            })
        } else if let Ok(name) = key.extract::<&str>() {
            self.inner
                .fields()
                .iter()
                .find(|f| f.name == name)
                .map(|f| Column { field: f.clone() })
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name.to_string()))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "schema indices must be integers or strings",
            ))
        }
    }

    /// Checks if a column name exists in the schema.
    fn __contains__(&self, name: &str) -> bool {
        self.inner.fields().iter().any(|f| f.name == name)
    }

    /// Iterates over the columns.
    fn __iter__(slf: PyRef<'_, Self>) -> PyResult<SchemaIter> {
        Ok(SchemaIter {
            schema: Arc::clone(&slf.inner),
            index: 0,
        })
    }

    fn __repr__(&self) -> String {
        let cols: Vec<String> = self
            .inner
            .fields()
            .iter()
            .map(|f| format!("('{}', '{}')", f.name, f.ty.type_name()))
            .collect();
        format!("Schema([{}])", cols.join(", "))
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// Iterator over schema columns.
#[pyclass]
pub struct SchemaIter {
    schema: Arc<RustSchema>,
    index: usize,
}

#[pymethods]
impl SchemaIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Column> {
        let fields = slf.schema.fields();
        if slf.index < fields.len() {
            let field = fields[slf.index].clone();
            slf.index += 1;
            Some(Column { field })
        } else {
            None
        }
    }
}
