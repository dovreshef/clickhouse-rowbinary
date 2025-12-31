"""Tests for Schema and Column classes."""

import pytest
from clickhouse_rowbinary import Column, Schema, SchemaError


class TestColumn:
    """Tests for Column class."""

    def test_create_column(self):
        col = Column("id", "UInt32")
        assert col.name == "id"
        assert col.type_str == "UInt32"

    def test_column_repr(self):
        col = Column("name", "String")
        assert repr(col) == "Column('name', 'String')"

    def test_column_equality(self):
        col1 = Column("id", "UInt32")
        col2 = Column("id", "UInt32")
        col3 = Column("id", "UInt64")
        assert col1 == col2
        assert col1 != col3

    def test_invalid_type_raises_error(self):
        with pytest.raises(SchemaError):
            Column("bad", "InvalidType")

    def test_complex_type(self):
        col = Column("data", "Nullable(Array(String))")
        assert col.name == "data"
        assert col.type_str == "Nullable(Array(String))"


class TestSchema:
    """Tests for Schema class."""

    def test_from_clickhouse(self):
        schema = Schema.from_clickhouse(
            [
                ("id", "UInt32"),
                ("name", "String"),
            ]
        )
        assert len(schema) == 2
        assert schema.names == ["id", "name"]

    def test_from_columns(self):
        cols = [Column("id", "UInt32"), Column("name", "String")]
        schema = Schema(cols)
        assert len(schema) == 2

    def test_columns_property(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        cols = schema.columns
        assert len(cols) == 1
        assert cols[0].name == "id"

    def test_getitem_by_index(self):
        schema = Schema.from_clickhouse(
            [
                ("id", "UInt32"),
                ("name", "String"),
            ]
        )
        assert schema[0].name == "id"
        assert schema[1].name == "name"
        assert schema[-1].name == "name"

    def test_getitem_by_name(self):
        schema = Schema.from_clickhouse(
            [
                ("id", "UInt32"),
                ("name", "String"),
            ]
        )
        assert schema["id"].type_str == "UInt32"
        assert schema["name"].type_str == "String"

    def test_getitem_invalid_index(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        with pytest.raises(IndexError):
            _ = schema[5]

    def test_getitem_invalid_name(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        with pytest.raises(KeyError):
            _ = schema["missing"]

    def test_contains(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        assert "id" in schema
        assert "missing" not in schema

    def test_iter(self):
        schema = Schema.from_clickhouse(
            [
                ("id", "UInt32"),
                ("name", "String"),
            ]
        )
        names = [col.name for col in schema]
        assert names == ["id", "name"]

    def test_bool_empty(self):
        schema = Schema([])
        assert not schema

    def test_bool_non_empty(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        assert schema

    def test_repr(self):
        schema = Schema.from_clickhouse([("id", "UInt32")])
        assert "Schema" in repr(schema)
        assert "id" in repr(schema)
        assert "UInt32" in repr(schema)

    def test_equality(self):
        s1 = Schema.from_clickhouse([("id", "UInt32")])
        s2 = Schema.from_clickhouse([("id", "UInt32")])
        s3 = Schema.from_clickhouse([("id", "UInt64")])
        assert s1 == s2
        assert s1 != s3

    def test_invalid_column_tuple(self):
        with pytest.raises(SchemaError):
            Schema.from_clickhouse([("id",)])  # type: ignore[list-item]  # Missing type

    def test_all_supported_types(self):
        """Test that all common ClickHouse types can be parsed."""
        types = [
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
            "Float32",
            "Float64",
            "Bool",
            "String",
            "FixedString(100)",
            "Date",
            "Date32",
            "DateTime",
            "DateTime('UTC')",
            "DateTime64(3)",
            "DateTime64(6, 'UTC')",
            "UUID",
            "IPv4",
            "IPv6",
            "Decimal(10, 2)",
            "Decimal32(2)",
            "Decimal64(4)",
            "Decimal128(6)",
            "Enum8('a' = 1, 'b' = 2)",
            "Enum16('x' = 100, 'y' = 200)",
            "Nullable(String)",
            "Array(UInt32)",
            "Array(Array(String))",
            "Map(String, Int32)",
            "Tuple(UInt32, String, Float64)",
            "LowCardinality(String)",
            "LowCardinality(Nullable(String))",
        ]
        for i, t in enumerate(types):
            schema = Schema.from_clickhouse([(f"col{i}", t)])
            assert len(schema) == 1
