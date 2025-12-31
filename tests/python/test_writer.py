"""Tests for RowBinaryWriter class."""

import pytest
from clickhouse_rowbinary import Format, RowBinaryWriter, Schema


class TestRowBinaryWriter:
    """Tests for RowBinaryWriter class."""

    def test_create_writer(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        assert writer.rows_written == 0

    def test_write_row_dict(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        assert writer.rows_written == 1

    def test_write_row_list(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row([1, b"Alice", True])
        assert writer.rows_written == 1

    def test_write_row_tuple(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row((1, b"Alice", True))
        assert writer.rows_written == 1

    def test_write_multiple_rows(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        writer.write_row({"id": 2, "name": b"Bob", "active": False})
        assert writer.rows_written == 2

    def test_write_rows(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        rows = [
            {"id": 1, "name": b"Alice", "active": True},
            {"id": 2, "name": b"Bob", "active": False},
            {"id": 3, "name": b"Charlie", "active": True},
        ]
        writer.write_rows(rows)
        assert writer.rows_written == 3

    def test_take_returns_bytes(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        data = writer.take()
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_take_resets_writer(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        _ = writer.take()
        assert writer.rows_written == 0

    def test_finish_alias(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        data = writer.finish()
        assert isinstance(data, bytes)
        assert writer.rows_written == 0

    def test_missing_column_raises_error(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        with pytest.raises(KeyError):
            writer.write_row({"id": 1, "name": b"Alice"})  # Missing 'active'

    def test_wrong_list_length_raises_error(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        with pytest.raises(ValueError):
            writer.write_row([1, b"Alice"])  # Missing one value

    def test_invalid_row_type_raises_error(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        with pytest.raises(TypeError):
            writer.write_row("not a dict, list, or tuple")  # type: ignore[arg-type]

    def test_context_manager(self, simple_schema):
        with RowBinaryWriter(simple_schema) as writer:
            writer.write_row({"id": 1, "name": b"Alice", "active": True})
            assert writer.rows_written == 1

    def test_repr(self, simple_schema):
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        assert "RowBinaryWriter" in repr(writer)
        assert "rows_written=1" in repr(writer)

    def test_format_rowbinary(self, simple_schema):
        writer = RowBinaryWriter(simple_schema, Format.RowBinary)
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        data = writer.take()
        assert len(data) > 0

    def test_format_with_names(self, simple_schema):
        writer = RowBinaryWriter(simple_schema, Format.RowBinaryWithNames)
        writer.write_header()
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        data = writer.take()
        # Header should contain column names
        assert b"id" in data
        assert b"name" in data
        assert b"active" in data

    def test_format_with_names_and_types(self, simple_schema):
        writer = RowBinaryWriter(simple_schema, Format.RowBinaryWithNamesAndTypes)
        writer.write_header()
        writer.write_row({"id": 1, "name": b"Alice", "active": True})
        data = writer.take()
        # Header should contain column names and types
        assert b"id" in data
        assert b"UInt32" in data

    def test_nullable_values(self, nullable_schema):
        writer = RowBinaryWriter(nullable_schema)
        writer.write_row({"id": 1, "name": b"Alice", "score": 95.5})
        writer.write_row({"id": 2, "name": None, "score": None})
        assert writer.rows_written == 2
        data = writer.take()
        assert len(data) > 0

    def test_string_as_str(self, simple_schema):
        """Test that str values are accepted for String columns."""
        writer = RowBinaryWriter(simple_schema)
        writer.write_row({"id": 1, "name": "Alice", "active": True})
        assert writer.rows_written == 1

    def test_array_values(self):
        schema = Schema.from_clickhouse([("tags", "Array(String)")])
        writer = RowBinaryWriter(schema)
        writer.write_row({"tags": [b"a", b"b", b"c"]})
        assert writer.rows_written == 1

    def test_map_values(self):
        schema = Schema.from_clickhouse([("meta", "Map(String, Int32)")])
        writer = RowBinaryWriter(schema)
        writer.write_row({"meta": {b"key1": 1, b"key2": 2}})
        assert writer.rows_written == 1

    def test_tuple_values(self):
        schema = Schema.from_clickhouse([("point", "Tuple(Float64, Float64)")])
        writer = RowBinaryWriter(schema)
        writer.write_row({"point": (1.5, 2.5)})
        assert writer.rows_written == 1
