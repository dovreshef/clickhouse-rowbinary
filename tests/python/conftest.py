"""Pytest configuration and fixtures."""

import pytest
from clickhouse_rowbinary import Schema


@pytest.fixture
def simple_schema() -> Schema:
    """A simple schema with basic types."""
    return Schema.from_clickhouse(
        [
            ("id", "UInt32"),
            ("name", "String"),
            ("active", "Bool"),
        ]
    )


@pytest.fixture
def nullable_schema() -> Schema:
    """A schema with nullable types."""
    return Schema.from_clickhouse(
        [
            ("id", "UInt32"),
            ("name", "Nullable(String)"),
            ("score", "Nullable(Float64)"),
        ]
    )


@pytest.fixture
def complex_schema() -> Schema:
    """A schema with complex types."""
    return Schema.from_clickhouse(
        [
            ("id", "UInt64"),
            ("tags", "Array(String)"),
            ("metadata", "Map(String, String)"),
            ("coords", "Tuple(Float64, Float64)"),
        ]
    )


@pytest.fixture
def all_primitives_schema() -> Schema:
    """A schema with all primitive types."""
    return Schema.from_clickhouse(
        [
            ("u8", "UInt8"),
            ("u16", "UInt16"),
            ("u32", "UInt32"),
            ("u64", "UInt64"),
            ("u128", "UInt128"),
            ("u256", "UInt256"),
            ("i8", "Int8"),
            ("i16", "Int16"),
            ("i32", "Int32"),
            ("i64", "Int64"),
            ("i128", "Int128"),
            ("i256", "Int256"),
            ("f32", "Float32"),
            ("f64", "Float64"),
            ("b", "Bool"),
            ("s", "String"),
            ("fs", "FixedString(10)"),
        ]
    )
