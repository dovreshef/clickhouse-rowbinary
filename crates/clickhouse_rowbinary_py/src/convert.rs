//! Python <-> Rust value conversion.

use std::net::{Ipv4Addr, Ipv6Addr};

use pyo3::{
    prelude::*,
    sync::GILOnceCell,
    types::{PyBool, PyBytes, PyDict, PyList, PyTuple, PyType},
};

use clickhouse_rowbinary::{DecimalSize, TypeDesc, Value};

use crate::errors::{EncodingError, ValidationError};

// Cached Python types for better performance
static DATETIME_DATE: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static DATETIME_DATETIME: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static UUID_UUID: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static IPV4_ADDRESS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static IPV6_ADDRESS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static DECIMAL_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static DECIMAL_ROUND_HALF_UP: GILOnceCell<Py<PyAny>> = GILOnceCell::new();
static ZONEINFO_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();

fn get_datetime_date(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DATETIME_DATE
        .get_or_try_init(py, || {
            py.import("datetime")?
                .getattr("date")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_datetime_datetime(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DATETIME_DATETIME
        .get_or_try_init(py, || {
            py.import("datetime")?
                .getattr("datetime")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_uuid_uuid(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    UUID_UUID
        .get_or_try_init(py, || {
            py.import("uuid")?
                .getattr("UUID")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_ipv4_address(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    IPV4_ADDRESS
        .get_or_try_init(py, || {
            py.import("ipaddress")?
                .getattr("IPv4Address")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_ipv6_address(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    IPV6_ADDRESS
        .get_or_try_init(py, || {
            py.import("ipaddress")?
                .getattr("IPv6Address")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_decimal_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DECIMAL_CLASS
        .get_or_try_init(py, || {
            py.import("decimal")?
                .getattr("Decimal")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

fn get_decimal_round_half_up(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    DECIMAL_ROUND_HALF_UP
        .get_or_try_init(py, || {
            py.import("decimal")?
                .getattr("ROUND_HALF_UP")
                .map(Bound::unbind)
        })
        .map(|t| t.bind(py))
}

fn get_zoneinfo(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    ZONEINFO_CLASS
        .get_or_try_init(py, || {
            py.import("zoneinfo")?
                .getattr("ZoneInfo")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(Into::into)
        })
        .map(|t| t.bind(py))
}

/// String handling mode for reading.
#[derive(Clone, Copy, Debug, Default)]
pub enum StringMode {
    /// Return strings as bytes (default, safe).
    #[default]
    Bytes,
    /// Decode strings as UTF-8.
    Str,
}

impl StringMode {
    pub fn from_str(s: &str) -> PyResult<Self> {
        match s {
            "bytes" => Ok(StringMode::Bytes),
            "str" => Ok(StringMode::Str),
            _ => Err(ValidationError::new_err(format!(
                "Invalid string_mode '{s}', expected 'bytes' or 'str'"
            ))),
        }
    }
}

/// Converts a Python object to a Rust Value based on the expected type.
#[allow(clippy::too_many_lines)]
pub fn python_to_value(py: Python<'_>, obj: &Bound<'_, PyAny>, ty: &TypeDesc) -> PyResult<Value> {
    match ty {
        TypeDesc::UInt8 => {
            let v: u8 = obj.extract()?;
            Ok(Value::UInt8(v))
        }
        TypeDesc::UInt16 => {
            let v: u16 = obj.extract()?;
            Ok(Value::UInt16(v))
        }
        TypeDesc::UInt32 => {
            let v: u32 = obj.extract()?;
            Ok(Value::UInt32(v))
        }
        TypeDesc::UInt64 => {
            let v: u64 = obj.extract()?;
            Ok(Value::UInt64(v))
        }
        TypeDesc::UInt128 => {
            let v: u128 = obj.extract()?;
            Ok(Value::UInt128(v))
        }
        TypeDesc::UInt256 => {
            // Convert Python int to 32-byte little-endian
            let int_obj = obj.call_method1("to_bytes", (32_usize, "little"))?;
            let bytes: Vec<u8> = int_obj.extract()?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(Value::UInt256(arr))
        }
        TypeDesc::Int8 => {
            let v: i8 = obj.extract()?;
            Ok(Value::Int8(v))
        }
        TypeDesc::Int16 => {
            let v: i16 = obj.extract()?;
            Ok(Value::Int16(v))
        }
        TypeDesc::Int32 => {
            let v: i32 = obj.extract()?;
            Ok(Value::Int32(v))
        }
        TypeDesc::Int64 => {
            let v: i64 = obj.extract()?;
            Ok(Value::Int64(v))
        }
        TypeDesc::Int128 => {
            let v: i128 = obj.extract()?;
            Ok(Value::Int128(v))
        }
        TypeDesc::Int256 => {
            // Convert Python int to 32-byte signed little-endian (two's complement)
            // Use kwargs for signed=True since it's a keyword-only argument
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("signed", true)?;
            let int_obj = obj.call_method("to_bytes", (32_usize, "little"), Some(&kwargs))?;
            let bytes: Vec<u8> = int_obj.extract()?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(Value::Int256(arr))
        }
        TypeDesc::Float32 => {
            let v: f32 = obj.extract()?;
            Ok(Value::Float32(v))
        }
        TypeDesc::Float64 => {
            let v: f64 = obj.extract()?;
            Ok(Value::Float64(v))
        }
        TypeDesc::Float16 => {
            let v: f32 = obj.extract()?;
            Ok(Value::Float16(v))
        }
        TypeDesc::BFloat16 => {
            let v: f32 = obj.extract()?;
            Ok(Value::BFloat16(v))
        }
        TypeDesc::Bool => {
            let v: bool = obj.extract()?;
            Ok(Value::Bool(v))
        }
        TypeDesc::String => {
            if let Ok(bytes) = obj.extract::<Vec<u8>>() {
                Ok(Value::String(bytes))
            } else if let Ok(s) = obj.extract::<String>() {
                Ok(Value::String(s.into_bytes()))
            } else {
                Err(ValidationError::new_err(
                    "Expected bytes or str for String type",
                ))
            }
        }
        TypeDesc::FixedString { length } => {
            let mut bytes: Vec<u8> = if let Ok(b) = obj.extract::<Vec<u8>>() {
                b
            } else if let Ok(s) = obj.extract::<String>() {
                s.into_bytes()
            } else {
                return Err(ValidationError::new_err(
                    "Expected bytes or str for FixedString type",
                ));
            };
            if bytes.len() > *length {
                return Err(ValidationError::new_err(format!(
                    "FixedString({length}) cannot hold {} bytes",
                    bytes.len()
                )));
            }
            // Pad with null bytes if shorter
            bytes.resize(*length, 0);
            Ok(Value::FixedString(bytes))
        }
        TypeDesc::Date => {
            // Accept datetime.date
            let date_type = get_datetime_date(py)?;
            if obj.is_instance(date_type)? {
                let year: i32 = obj.getattr("year")?.extract()?;
                let month: u32 = obj.getattr("month")?.extract()?;
                let day: u32 = obj.getattr("day")?.extract()?;
                // Days since 1970-01-01
                let days = days_since_epoch(year, month, day);
                Ok(Value::Date(days as u16))
            } else {
                Err(ValidationError::new_err(
                    "Expected datetime.date for Date type",
                ))
            }
        }
        TypeDesc::Date32 => {
            let date_type = get_datetime_date(py)?;
            if obj.is_instance(date_type)? {
                let year: i32 = obj.getattr("year")?.extract()?;
                let month: u32 = obj.getattr("month")?.extract()?;
                let day: u32 = obj.getattr("day")?.extract()?;
                let days = days_since_epoch(year, month, day);
                Ok(Value::Date32(days))
            } else {
                Err(ValidationError::new_err(
                    "Expected datetime.date for Date32 type",
                ))
            }
        }
        TypeDesc::DateTime { .. } => {
            let datetime_type = get_datetime_datetime(py)?;
            if obj.is_instance(datetime_type)? {
                let ts: f64 = obj.call_method0("timestamp")?.extract()?;
                Ok(Value::DateTime(ts as u32))
            } else {
                Err(ValidationError::new_err(
                    "Expected datetime.datetime for DateTime type",
                ))
            }
        }
        TypeDesc::DateTime64 { precision, .. } => {
            let datetime_type = get_datetime_datetime(py)?;
            if obj.is_instance(datetime_type)? {
                let ts: f64 = obj.call_method0("timestamp")?.extract()?;
                let multiplier = 10_i64.pow(*precision as u32);
                let value = (ts * multiplier as f64) as i64;
                Ok(Value::DateTime64(value))
            } else {
                Err(ValidationError::new_err(
                    "Expected datetime.datetime for DateTime64 type",
                ))
            }
        }
        TypeDesc::Uuid => {
            let uuid_type = get_uuid_uuid(py)?;
            if obj.is_instance(uuid_type)? {
                let bytes: [u8; 16] = obj.getattr("bytes")?.extract()?;
                Ok(Value::Uuid(uuid::Uuid::from_bytes(bytes)))
            } else {
                Err(ValidationError::new_err("Expected uuid.UUID for UUID type"))
            }
        }
        TypeDesc::Ipv4 => {
            let ipv4_type = get_ipv4_address(py)?;
            if obj.is_instance(ipv4_type)? {
                let packed: [u8; 4] = obj.getattr("packed")?.extract()?;
                Ok(Value::Ipv4(Ipv4Addr::from(packed)))
            } else {
                Err(ValidationError::new_err(
                    "Expected ipaddress.IPv4Address for IPv4 type",
                ))
            }
        }
        TypeDesc::Ipv6 => {
            let ipv6_type = get_ipv6_address(py)?;
            if obj.is_instance(ipv6_type)? {
                let packed: [u8; 16] = obj.getattr("packed")?.extract()?;
                Ok(Value::Ipv6(Ipv6Addr::from(packed)))
            } else {
                Err(ValidationError::new_err(
                    "Expected ipaddress.IPv6Address for IPv6 type",
                ))
            }
        }
        TypeDesc::Decimal32 { scale } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            if val < i128::from(i32::MIN) || val > i128::from(i32::MAX) {
                return Err(ValidationError::new_err("Value overflow for Decimal32"));
            }
            Ok(Value::Decimal32(val as i32))
        }
        TypeDesc::Decimal64 { scale } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            if val < i128::from(i64::MIN) || val > i128::from(i64::MAX) {
                return Err(ValidationError::new_err("Value overflow for Decimal64"));
            }
            Ok(Value::Decimal64(val as i64))
        }
        TypeDesc::Decimal128 { scale, .. } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            Ok(Value::Decimal128(val))
        }
        TypeDesc::Decimal256 { scale, .. } => {
            let bytes = python_to_decimal256(py, obj, *scale)?;
            Ok(Value::Decimal256(bytes))
        }
        TypeDesc::Decimal {
            scale,
            size: DecimalSize::Bits32,
            ..
        } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            if val < i128::from(i32::MIN) || val > i128::from(i32::MAX) {
                return Err(ValidationError::new_err("Value overflow for Decimal32"));
            }
            Ok(Value::Decimal32(val as i32))
        }
        TypeDesc::Decimal {
            scale,
            size: DecimalSize::Bits64,
            ..
        } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            if val < i128::from(i64::MIN) || val > i128::from(i64::MAX) {
                return Err(ValidationError::new_err("Value overflow for Decimal64"));
            }
            Ok(Value::Decimal64(val as i64))
        }
        TypeDesc::Decimal {
            scale,
            size: DecimalSize::Bits128,
            ..
        } => {
            let val = python_to_decimal_scaled(py, obj, *scale)?;
            Ok(Value::Decimal128(val))
        }
        TypeDesc::Decimal {
            scale,
            size: DecimalSize::Bits256,
            ..
        } => {
            let bytes = python_to_decimal256(py, obj, *scale)?;
            Ok(Value::Decimal256(bytes))
        }
        TypeDesc::Enum8(variants) => {
            // Accept string (name) and look up value
            let name: &str = obj.extract()?;
            for (n, v) in variants {
                if n == name {
                    return Ok(Value::Enum8(*v));
                }
            }
            Err(ValidationError::new_err(format!(
                "Unknown enum variant '{name}'"
            )))
        }
        TypeDesc::Enum16(variants) => {
            let name: &str = obj.extract()?;
            for (n, v) in variants {
                if n == name {
                    return Ok(Value::Enum16(*v));
                }
            }
            Err(ValidationError::new_err(format!(
                "Unknown enum variant '{name}'"
            )))
        }
        TypeDesc::Nullable(inner) => {
            if obj.is_none() {
                Ok(Value::Nullable(None))
            } else {
                let inner_value = python_to_value(py, obj, inner)?;
                Ok(Value::Nullable(Some(Box::new(inner_value))))
            }
        }
        TypeDesc::LowCardinality(inner) => {
            // LowCardinality is transparent
            python_to_value(py, obj, inner)
        }
        TypeDesc::Array(inner) => {
            let items: Vec<Bound<'_, PyAny>> = obj.extract()?;
            let mut values = Vec::with_capacity(items.len());
            for item in &items {
                values.push(python_to_value(py, item, inner)?);
            }
            Ok(Value::Array(values))
        }
        TypeDesc::Map { key, value } => {
            let dict: &Bound<'_, PyDict> = obj.downcast()?;
            let mut entries = Vec::with_capacity(dict.len());
            for (k, v) in dict.iter() {
                let key_val = python_to_value(py, &k, key)?;
                let val_val = python_to_value(py, &v, value)?;
                entries.push((key_val, val_val));
            }
            Ok(Value::Map(entries))
        }
        TypeDesc::Tuple(items) => {
            let tuple: Vec<Bound<'_, PyAny>> = obj.extract()?;
            if tuple.len() != items.len() {
                return Err(ValidationError::new_err(format!(
                    "Tuple length mismatch: expected {}, got {}",
                    items.len(),
                    tuple.len()
                )));
            }
            let mut values = Vec::with_capacity(items.len());
            for (item, ty_item) in tuple.iter().zip(items.iter()) {
                values.push(python_to_value(py, item, &ty_item.ty)?);
            }
            Ok(Value::Tuple(values))
        }
        _ => Err(EncodingError::new_err(format!(
            "Unsupported type for encoding: {}",
            ty.type_name()
        ))),
    }
}

/// Converts a Rust Value to a Python object.
#[allow(clippy::too_many_lines)]
pub fn value_to_python(
    py: Python<'_>,
    value: &Value,
    ty: &TypeDesc,
    string_mode: StringMode,
) -> PyResult<Py<PyAny>> {
    match (value, ty) {
        (Value::UInt8(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::UInt16(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::UInt32(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::UInt64(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::UInt128(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::UInt256(bytes), _) => {
            // Convert 32-byte little-endian to Python int
            let int_type = py.get_type::<pyo3::types::PyInt>();
            let py_bytes = PyBytes::new(py, bytes);
            let result = int_type.call_method1("from_bytes", (py_bytes, "little"))?;
            Ok(result.unbind())
        }
        (Value::Int8(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Int16(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Int32(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Int64(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Int128(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Int256(bytes), _) => {
            // Convert 32-byte signed little-endian (two's complement) to Python int
            let int_type = py.get_type::<pyo3::types::PyInt>();
            let py_bytes = PyBytes::new(py, bytes);
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("signed", true)?;
            let result = int_type.call_method("from_bytes", (py_bytes, "little"), Some(&kwargs))?;
            Ok(result.unbind())
        }
        (Value::Float32(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Float64(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Float16(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::BFloat16(v), _) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        (Value::Bool(v), _) => Ok(PyBool::new(py, *v).to_owned().into_any().unbind()),
        (Value::String(bytes) | Value::FixedString(bytes), _) => match string_mode {
            StringMode::Bytes => Ok(PyBytes::new(py, bytes).into_any().unbind()),
            StringMode::Str => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| ValidationError::new_err(format!("Invalid UTF-8: {e}")))?;
                Ok(s.into_pyobject(py)?.into_any().unbind())
            }
        },
        (Value::Date(days), _) => {
            let (year, month, day) = date_from_days(*days as i32);
            let date_cls = get_datetime_date(py)?;
            let date = date_cls.call1((year, month, day))?;
            Ok(date.unbind())
        }
        (Value::Date32(days), _) => {
            let (year, month, day) = date_from_days(*days);
            let date_cls = get_datetime_date(py)?;
            let date = date_cls.call1((year, month, day))?;
            Ok(date.unbind())
        }
        (Value::DateTime(ts), TypeDesc::DateTime { timezone }) => {
            let datetime_cls = get_datetime_datetime(py)?;
            let dt = datetime_cls.call_method1("fromtimestamp", (*ts as f64,))?;
            if let Some(tz_name) = timezone {
                let zoneinfo_cls = get_zoneinfo(py)?;
                let tz = zoneinfo_cls.call1((tz_name,))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("tzinfo", tz)?;
                let dt = dt.call_method("replace", (), Some(&kwargs))?;
                Ok(dt.unbind())
            } else {
                Ok(dt.unbind())
            }
        }
        (
            Value::DateTime64(ts),
            TypeDesc::DateTime64 {
                precision,
                timezone,
            },
        ) => {
            let datetime_cls = get_datetime_datetime(py)?;
            let multiplier = 10_i64.pow(*precision as u32);
            let seconds = *ts as f64 / multiplier as f64;
            let dt = datetime_cls.call_method1("fromtimestamp", (seconds,))?;
            if let Some(tz_name) = timezone {
                let zoneinfo_cls = get_zoneinfo(py)?;
                let tz = zoneinfo_cls.call1((tz_name,))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("tzinfo", tz)?;
                let dt = dt.call_method("replace", (), Some(&kwargs))?;
                Ok(dt.unbind())
            } else {
                Ok(dt.unbind())
            }
        }
        (Value::Uuid(uuid), _) => {
            let uuid_cls = get_uuid_uuid(py)?;
            let kwargs = PyDict::new(py);
            let bytes = PyBytes::new(py, uuid.as_bytes());
            kwargs.set_item("bytes", bytes)?;
            let uuid_obj = uuid_cls.call((), Some(&kwargs))?;
            Ok(uuid_obj.unbind())
        }
        (Value::Ipv4(addr), _) => {
            let ipv4_cls = get_ipv4_address(py)?;
            let packed = PyBytes::new(py, &addr.octets());
            let addr_obj = ipv4_cls.call1((packed,))?;
            Ok(addr_obj.unbind())
        }
        (Value::Ipv6(addr), _) => {
            let ipv6_cls = get_ipv6_address(py)?;
            let packed = PyBytes::new(py, &addr.octets());
            let addr_obj = ipv6_cls.call1((packed,))?;
            Ok(addr_obj.unbind())
        }
        (Value::Enum8(v), TypeDesc::Enum8(variants)) => {
            for (name, value) in variants {
                if value == v {
                    return Ok(name.clone().into_pyobject(py)?.into_any().unbind());
                }
            }
            Err(ValidationError::new_err(format!(
                "Unknown Enum8 value: {v}"
            )))
        }
        (Value::Enum16(v), TypeDesc::Enum16(variants)) => {
            for (name, value) in variants {
                if value == v {
                    return Ok(name.clone().into_pyobject(py)?.into_any().unbind());
                }
            }
            Err(ValidationError::new_err(format!(
                "Unknown Enum16 value: {v}"
            )))
        }
        // Decimal types
        (Value::Decimal32(v), TypeDesc::Decimal32 { scale }) => {
            decimal_to_python(py, i128::from(*v), *scale)
        }
        (Value::Decimal64(v), TypeDesc::Decimal64 { scale }) => {
            decimal_to_python(py, i128::from(*v), *scale)
        }
        (Value::Decimal128(v), TypeDesc::Decimal128 { scale, .. }) => {
            decimal_to_python(py, *v, *scale)
        }
        (Value::Decimal256(bytes), TypeDesc::Decimal256 { scale, .. }) => {
            decimal256_to_python(py, bytes, *scale)
        }
        // Generic Decimal based on precision
        (Value::Decimal32(v), TypeDesc::Decimal { scale, .. }) => {
            decimal_to_python(py, i128::from(*v), *scale)
        }
        (Value::Decimal64(v), TypeDesc::Decimal { scale, .. }) => {
            decimal_to_python(py, i128::from(*v), *scale)
        }
        (Value::Decimal128(v), TypeDesc::Decimal { scale, .. }) => {
            decimal_to_python(py, *v, *scale)
        }
        (Value::Decimal256(bytes), TypeDesc::Decimal { scale, .. }) => {
            decimal256_to_python(py, bytes, *scale)
        }
        (Value::Nullable(None), _) => Ok(py.None()),
        (Value::Nullable(Some(inner)), TypeDesc::Nullable(inner_ty)) => {
            value_to_python(py, inner, inner_ty, string_mode)
        }
        (Value::Array(values), TypeDesc::Array(inner_ty)) => {
            let list = PyList::empty(py);
            for v in values {
                let py_val = value_to_python(py, v, inner_ty, string_mode)?;
                list.append(py_val)?;
            }
            Ok(list.into_any().unbind())
        }
        (Value::Map(entries), TypeDesc::Map { key, value }) => {
            let dict = PyDict::new(py);
            for (k, v) in entries {
                let py_key = value_to_python(py, k, key, string_mode)?;
                let py_val = value_to_python(py, v, value, string_mode)?;
                dict.set_item(py_key, py_val)?;
            }
            Ok(dict.into_any().unbind())
        }
        (Value::Tuple(values), TypeDesc::Tuple(items)) => {
            let mut py_values = Vec::with_capacity(values.len());
            for (v, item) in values.iter().zip(items.iter()) {
                py_values.push(value_to_python(py, v, &item.ty, string_mode)?);
            }
            let tuple = PyTuple::new(py, py_values)?;
            Ok(tuple.into_any().unbind())
        }
        (value, ty) => {
            // Pass through LowCardinality
            if let TypeDesc::LowCardinality(inner) = ty {
                return value_to_python(py, value, inner, string_mode);
            }
            Err(ValidationError::new_err(format!(
                "Cannot convert value {} for type {}",
                value.type_name(),
                ty.type_name()
            )))
        }
    }
}

/// JDN of Unix epoch (1970-01-01).
const UNIX_EPOCH_JDN: i32 = 2_440_588;

/// Days since Unix epoch (1970-01-01).
fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    // Simple calculation using the formula for Julian Day Number
    let adj = (14 - month as i32) / 12;
    let yr = year + 4800 - adj;
    let mon = month as i32 + 12 * adj - 3;
    let jdn = day as i32 + (153 * mon + 2) / 5 + 365 * yr + yr / 4 - yr / 100 + yr / 400 - 32045;
    jdn - UNIX_EPOCH_JDN
}

/// Date from days since Unix epoch.
#[allow(clippy::many_single_char_names)]
fn date_from_days(days: i32) -> (i32, u32, u32) {
    // Standard algorithm for converting Julian Day Number to calendar date
    let jdn = days + UNIX_EPOCH_JDN;
    let f = jdn + 68569;
    let c = (4 * f) / 146_097;
    let d = f - (146_097 * c + 3) / 4;
    let e = (4000 * (d + 1)) / 1_461_001;
    let g = d - (1461 * e) / 4 + 31;
    let h = (80 * g) / 2447;
    let day = g - (2447 * h) / 80;
    let i = h / 11;
    let month = h + 2 - 12 * i;
    let year = 100 * (c - 49) + e + i;
    (year, month as u32, day as u32)
}

/// Converts a Python value to a scaled decimal integer (for Decimal32/64/128).
fn python_to_decimal_scaled(py: Python<'_>, obj: &Bound<'_, PyAny>, scale: u8) -> PyResult<i128> {
    let decimal_cls = get_decimal_class(py)?;

    // Convert to Decimal via string (avoid float precision loss)
    let decimal_val = if obj.is_instance(decimal_cls)? {
        obj.clone().unbind()
    } else {
        decimal_cls.call1((obj.str()?,))?.unbind()
    };

    // Quantize to required scale
    let scale_template = decimal_cls.call1((format!("1e-{scale}"),))?;
    let rounding = get_decimal_round_half_up(py)?;
    let rounded = decimal_val
        .bind(py)
        .call_method1("quantize", (scale_template, rounding))?;

    // Scale up to integer
    let scale_factor = 10_i128.pow(u32::from(scale));
    let scaled = rounded.call_method1("__mul__", (scale_factor,))?;
    scaled.call_method0("__int__")?.extract()
}

/// Converts a Python value to Decimal256 bytes (little-endian two's
/// complement).
fn python_to_decimal256(py: Python<'_>, obj: &Bound<'_, PyAny>, scale: u8) -> PyResult<[u8; 32]> {
    use num_bigint::BigInt;

    let decimal_cls = get_decimal_class(py)?;

    // Convert to Decimal via string
    let decimal_val = if obj.is_instance(decimal_cls)? {
        obj.clone().unbind()
    } else {
        decimal_cls.call1((obj.str()?,))?.unbind()
    };

    // Quantize to required scale
    let scale_template = decimal_cls.call1((format!("1e-{scale}"),))?;
    let rounding = get_decimal_round_half_up(py)?;
    let rounded = decimal_val
        .bind(py)
        .call_method1("quantize", (scale_template, rounding))?;

    // Get the scaled integer as a string
    let scale_factor_str = format!("1{}", "0".repeat(scale as usize));
    let scale_factor = decimal_cls.call1((scale_factor_str,))?;
    let scaled = rounded.call_method1("__mul__", (scale_factor,))?;
    let int_val = scaled.call_method0("__int__")?;
    let int_str: String = int_val.str()?.extract()?;

    // Parse as BigInt and convert to bytes
    let big_int: BigInt = int_str
        .parse()
        .map_err(|_| ValidationError::new_err("Failed to parse decimal value"))?;
    let bytes = big_int.to_signed_bytes_le();

    // Pad or truncate to 32 bytes
    let mut result = [0u8; 32];
    let is_negative = big_int.sign() == num_bigint::Sign::Minus;

    if bytes.len() > 32 {
        return Err(ValidationError::new_err("Value overflow for Decimal256"));
    }

    result[..bytes.len()].copy_from_slice(&bytes);
    // Sign extend if negative
    if is_negative {
        result[bytes.len()..].fill(0xff);
    }

    Ok(result)
}

/// Converts a Decimal32/64/128 value to Python decimal.Decimal.
fn decimal_to_python(py: Python<'_>, value: i128, scale: u8) -> PyResult<Py<PyAny>> {
    let sign = if value < 0 { "-" } else { "" };
    let abs_val = value.unsigned_abs();
    let s = abs_val.to_string();
    let scale = scale as usize;

    let decimal_str = if scale == 0 {
        format!("{sign}{s}")
    } else if s.len() <= scale {
        format!("{sign}0.{abs_val:0>scale$}")
    } else {
        let split = s.len() - scale;
        format!("{sign}{}.{}", &s[..split], &s[split..])
    };

    let decimal_cls = get_decimal_class(py)?;
    Ok(decimal_cls.call1((decimal_str,))?.unbind())
}

/// Converts a Decimal256 value to Python decimal.Decimal.
fn decimal256_to_python(py: Python<'_>, bytes: &[u8; 32], scale: u8) -> PyResult<Py<PyAny>> {
    use num_bigint::BigInt;

    let big_int = BigInt::from_signed_bytes_le(bytes);
    let int_str = big_int.to_string();

    let sign = if big_int.sign() == num_bigint::Sign::Minus {
        "-"
    } else {
        ""
    };
    let abs_str = int_str.trim_start_matches('-');
    let scale = scale as usize;

    let decimal_str = if scale == 0 {
        format!("{sign}{abs_str}")
    } else if abs_str.len() <= scale {
        format!("{sign}0.{abs_str:0>scale$}")
    } else {
        let split = abs_str.len() - scale;
        format!("{sign}{}.{}", &abs_str[..split], &abs_str[split..])
    };

    let decimal_cls = get_decimal_class(py)?;
    Ok(decimal_cls.call1((decimal_str,))?.unbind())
}
