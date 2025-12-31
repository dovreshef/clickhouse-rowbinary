"""Tests for roundtrip encoding/decoding."""

from clickhouse_rowbinary import Format, RowBinaryReader, RowBinaryWriter, Schema


def roundtrip(schema, rows, format=Format.RowBinary, write_header=False):
    """Encode rows and decode them back."""
    writer = RowBinaryWriter(schema, format)
    if write_header:
        writer.write_header()
    writer.write_rows(rows)
    data = writer.take()

    reader = RowBinaryReader(data, schema, format)
    return reader.read_all()


class TestBasicRoundtrip:
    """Basic roundtrip tests."""

    def test_empty_data(self, simple_schema):
        result = roundtrip(simple_schema, [])
        assert result == []

    def test_single_row(self, simple_schema):
        rows = [{"id": 1, "name": b"Alice", "active": True}]
        result = roundtrip(simple_schema, rows)
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == b"Alice"
        assert result[0]["active"] is True

    def test_multiple_rows(self, simple_schema):
        rows = [
            {"id": 1, "name": b"Alice", "active": True},
            {"id": 2, "name": b"Bob", "active": False},
            {"id": 3, "name": b"Charlie", "active": True},
        ]
        result = roundtrip(simple_schema, rows)
        assert len(result) == 3
        for i, row in enumerate(result):
            assert row["id"] == rows[i]["id"]
            assert row["name"] == rows[i]["name"]
            assert row["active"] == rows[i]["active"]

    def test_format_with_names(self, simple_schema):
        rows = [{"id": 1, "name": b"Alice", "active": True}]
        result = roundtrip(
            simple_schema, rows, Format.RowBinaryWithNames, write_header=True
        )
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_format_with_names_and_types(self, simple_schema):
        rows = [{"id": 1, "name": b"Alice", "active": True}]
        result = roundtrip(
            simple_schema, rows, Format.RowBinaryWithNamesAndTypes, write_header=True
        )
        assert len(result) == 1
        assert result[0]["id"] == 1


class TestNullableRoundtrip:
    """Roundtrip tests for nullable values."""

    def test_nullable_with_values(self, nullable_schema):
        rows = [
            {"id": 1, "name": b"Alice", "score": 95.5},
            {"id": 2, "name": b"Bob", "score": 88.0},
        ]
        result = roundtrip(nullable_schema, rows)
        assert result[0]["name"] == b"Alice"
        assert result[0]["score"] == 95.5

    def test_nullable_with_nulls(self, nullable_schema):
        rows = [
            {"id": 1, "name": None, "score": None},
            {"id": 2, "name": b"Bob", "score": None},
            {"id": 3, "name": None, "score": 77.5},
        ]
        result = roundtrip(nullable_schema, rows)
        assert result[0]["name"] is None
        assert result[0]["score"] is None
        assert result[1]["name"] == b"Bob"
        assert result[1]["score"] is None
        assert result[2]["name"] is None
        assert result[2]["score"] == 77.5


class TestComplexTypeRoundtrip:
    """Roundtrip tests for complex types."""

    def test_array(self):
        schema = Schema.from_clickhouse([("tags", "Array(String)")])
        rows = [
            {"tags": [b"a", b"b", b"c"]},
            {"tags": []},
            {"tags": [b"single"]},
        ]
        result = roundtrip(schema, rows)
        assert result[0]["tags"] == [b"a", b"b", b"c"]
        assert result[1]["tags"] == []
        assert result[2]["tags"] == [b"single"]

    def test_nested_array(self):
        schema = Schema.from_clickhouse([("matrix", "Array(Array(UInt32))")])
        rows = [
            {"matrix": [[1, 2], [3, 4, 5], []]},
        ]
        result = roundtrip(schema, rows)
        assert result[0]["matrix"] == [[1, 2], [3, 4, 5], []]

    def test_map(self):
        schema = Schema.from_clickhouse([("meta", "Map(String, Int32)")])
        rows = [
            {"meta": {b"a": 1, b"b": 2}},
            {"meta": {}},
        ]
        result = roundtrip(schema, rows)
        assert result[0]["meta"] == {b"a": 1, b"b": 2}
        assert result[1]["meta"] == {}

    def test_tuple(self):
        schema = Schema.from_clickhouse([("point", "Tuple(Float64, Float64, String)")])
        rows = [
            {"point": (1.5, 2.5, b"label")},
        ]
        result = roundtrip(schema, rows)
        assert result[0]["point"] == (1.5, 2.5, b"label")


class TestLargeDataRoundtrip:
    """Roundtrip tests for larger data sets."""

    def test_many_rows(self, simple_schema):
        rows = [
            {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
            for i in range(1000)
        ]
        result = roundtrip(simple_schema, rows)
        assert len(result) == 1000
        for i, row in enumerate(result):
            assert row["id"] == i
            assert row["name"] == f"user{i}".encode()
            assert row["active"] == (i % 2 == 0)

    def test_large_strings(self):
        schema = Schema.from_clickhouse([("data", "String")])
        large_string = b"x" * 100000
        rows = [{"data": large_string}]
        result = roundtrip(schema, rows)
        assert result[0]["data"] == large_string

    def test_large_arrays(self):
        schema = Schema.from_clickhouse([("numbers", "Array(UInt32)")])
        large_array = list(range(10000))
        rows = [{"numbers": large_array}]
        result = roundtrip(schema, rows)
        assert result[0]["numbers"] == large_array
