"""Tests for RowBinaryReader and Row classes."""

import pytest
from clickhouse_rowbinary import (
    Format,
    RowBinaryReader,
    RowBinaryWriter,
    ValidationError,
)


def encode_rows(schema, rows, format=Format.RowBinary, write_header=False):
    """Helper to encode rows for testing."""
    writer = RowBinaryWriter(schema, format)
    if write_header:
        writer.write_header()
    writer.write_rows(rows)
    return writer.take()


class TestRowBinaryReader:
    """Tests for RowBinaryReader class."""

    def test_create_reader(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        assert reader.schema == simple_schema

    def test_read_single_row(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row["id"] == 1
        assert row["name"] == b"Alice"
        assert row["active"] is True

    def test_read_row_returns_none_at_end(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        _ = reader.read_row()
        assert reader.read_row() is None

    def test_read_all(self, simple_schema):
        rows = [
            {"id": 1, "name": b"Alice", "active": True},
            {"id": 2, "name": b"Bob", "active": False},
        ]
        data = encode_rows(simple_schema, rows)
        reader = RowBinaryReader(data, simple_schema)
        result = reader.read_all()
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2

    def test_iterate(self, simple_schema):
        rows = [
            {"id": 1, "name": b"Alice", "active": True},
            {"id": 2, "name": b"Bob", "active": False},
        ]
        data = encode_rows(simple_schema, rows)
        reader = RowBinaryReader(data, simple_schema)
        result = list(reader)
        assert len(result) == 2

    def test_string_mode_bytes(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema, string_mode="bytes")
        row = reader.read_row()
        assert row is not None
        assert isinstance(row["name"], bytes)

    def test_string_mode_str(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema, string_mode="str")
        row = reader.read_row()
        assert row is not None
        assert isinstance(row["name"], str)
        assert row["name"] == "Alice"

    def test_format_with_names(self, simple_schema):
        data = encode_rows(
            simple_schema,
            [{"id": 1, "name": b"Alice", "active": True}],
            Format.RowBinaryWithNames,
            write_header=True,
        )
        reader = RowBinaryReader(data, simple_schema, Format.RowBinaryWithNames)
        row = reader.read_row()
        assert row is not None
        assert row["id"] == 1

    def test_format_with_names_and_types(self, simple_schema):
        data = encode_rows(
            simple_schema,
            [{"id": 1, "name": b"Alice", "active": True}],
            Format.RowBinaryWithNamesAndTypes,
            write_header=True,
        )
        reader = RowBinaryReader(data, simple_schema, Format.RowBinaryWithNamesAndTypes)
        row = reader.read_row()
        assert row is not None
        assert row["id"] == 1

    def test_repr(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 1, "name": b"Alice", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        assert "RowBinaryReader" in repr(reader)


class TestRow:
    """Tests for Row class."""

    def test_getitem_by_name(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row["id"] == 42
        assert row["name"] == b"Test"
        assert row["active"] is True

    def test_getitem_by_index(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row[0] == 42
        assert row[1] == b"Test"
        assert row[2] is True

    def test_getitem_negative_index(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row[-1] is True
        assert row[-2] == b"Test"

    def test_getitem_invalid_index(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        with pytest.raises(IndexError):
            _ = row[10]

    def test_getitem_invalid_name(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        with pytest.raises(KeyError):
            _ = row["missing"]

    def test_get_with_default(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row.get("id") == 42
        assert row.get("missing") is None
        assert row.get("missing", "default") == "default"

    def test_get_str(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row.get_str("name") == "Test"

    def test_get_str_invalid_utf8_strict(self, simple_schema):
        # Write invalid UTF-8 bytes
        data = encode_rows(
            simple_schema, [{"id": 42, "name": b"\xff\xfe", "active": True}]
        )
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        # May raise UnicodeDecodeError or a wrapper exception
        with pytest.raises((UnicodeDecodeError, Exception)):
            row.get_str("name", errors="strict")

    def test_get_str_invalid_utf8_replace(self, simple_schema):
        data = encode_rows(
            simple_schema, [{"id": 42, "name": b"\xff\xfe", "active": True}]
        )
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        result = row.get_str("name", errors="replace")
        assert "\ufffd" in result  # Replacement character

    def test_get_str_non_string_column(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        with pytest.raises(ValidationError):
            row.get_str("id")  # Not a string column

    def test_attribute_access(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row.id == 42
        assert row.name == b"Test"
        assert row.active is True

    def test_attribute_access_invalid(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        with pytest.raises(AttributeError):
            _ = row.missing

    def test_as_dict(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        d = row.as_dict()
        assert d == {"id": 42, "name": b"Test", "active": True}

    def test_as_tuple(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        t = row.as_tuple()
        assert t == (42, b"Test", True)

    def test_keys(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert row.keys() == ["id", "name", "active"]

    def test_values(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert list(row.values()) == [42, b"Test", True]

    def test_items(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        items = row.items()
        assert items == [("id", 42), ("name", b"Test"), ("active", True)]

    def test_len(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert len(row) == 3

    def test_contains(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert "id" in row
        assert "missing" not in row

    def test_iter(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        assert row is not None
        assert list(row) == ["id", "name", "active"]

    def test_repr(self, simple_schema):
        data = encode_rows(simple_schema, [{"id": 42, "name": b"Test", "active": True}])
        reader = RowBinaryReader(data, simple_schema)
        row = reader.read_row()
        r = repr(row)
        assert "Row" in r
        assert "id" in r


class TestNullableValues:
    """Tests for nullable value handling."""

    def test_read_nullable_with_value(self, nullable_schema):
        data = encode_rows(
            nullable_schema, [{"id": 1, "name": b"Alice", "score": 95.5}]
        )
        reader = RowBinaryReader(data, nullable_schema)
        row = reader.read_row()
        assert row is not None
        assert row["name"] == b"Alice"
        assert row["score"] == 95.5

    def test_read_nullable_with_none(self, nullable_schema):
        data = encode_rows(nullable_schema, [{"id": 1, "name": None, "score": None}])
        reader = RowBinaryReader(data, nullable_schema)
        row = reader.read_row()
        assert row is not None
        assert row["name"] is None
        assert row["score"] is None
