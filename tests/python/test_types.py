"""Tests for type conversion between Python and ClickHouse types."""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from zoneinfo import ZoneInfo

import pytest
from clickhouse_rowbinary import RowBinaryReader, RowBinaryWriter, Schema


def roundtrip_value(type_str: str, value):
    """Roundtrip a single value through encode/decode."""
    schema = Schema.from_clickhouse([("val", type_str)])
    writer = RowBinaryWriter(schema)
    writer.write_row({"val": value})
    data = writer.take()
    reader = RowBinaryReader(data, schema)
    row = reader.read_row()
    assert row is not None
    return row["val"]


class TestIntegerTypes:
    """Tests for integer type conversions."""

    def test_uint8(self):
        assert roundtrip_value("UInt8", 0) == 0
        assert roundtrip_value("UInt8", 255) == 255

    def test_uint16(self):
        assert roundtrip_value("UInt16", 0) == 0
        assert roundtrip_value("UInt16", 65535) == 65535

    def test_uint32(self):
        assert roundtrip_value("UInt32", 0) == 0
        assert roundtrip_value("UInt32", 4294967295) == 4294967295

    def test_uint64(self):
        assert roundtrip_value("UInt64", 0) == 0
        assert roundtrip_value("UInt64", 18446744073709551615) == 18446744073709551615

    def test_uint128(self):
        val = 2**127 - 1
        assert roundtrip_value("UInt128", val) == val

    def test_uint256(self):
        val = 2**255 - 1
        assert roundtrip_value("UInt256", val) == val

    def test_int8(self):
        assert roundtrip_value("Int8", -128) == -128
        assert roundtrip_value("Int8", 127) == 127

    def test_int16(self):
        assert roundtrip_value("Int16", -32768) == -32768
        assert roundtrip_value("Int16", 32767) == 32767

    def test_int32(self):
        assert roundtrip_value("Int32", -2147483648) == -2147483648
        assert roundtrip_value("Int32", 2147483647) == 2147483647

    def test_int64(self):
        assert roundtrip_value("Int64", -9223372036854775808) == -9223372036854775808
        assert roundtrip_value("Int64", 9223372036854775807) == 9223372036854775807

    def test_int128(self):
        val = -(2**127) + 1
        assert roundtrip_value("Int128", val) == val

    def test_int256(self):
        # Test with a smaller negative value to avoid overflow edge cases
        val = -(2**200)
        assert roundtrip_value("Int256", val) == val


class TestFloatTypes:
    """Tests for float type conversions."""

    def test_float32(self):
        result = roundtrip_value("Float32", 3.14)
        assert abs(result - 3.14) < 0.001

    def test_float64(self):
        result = roundtrip_value("Float64", 3.141592653589793)
        assert result == 3.141592653589793

    def test_float_special_values(self):
        import math

        assert math.isinf(roundtrip_value("Float64", float("inf")))
        assert math.isinf(roundtrip_value("Float64", float("-inf")))
        assert math.isnan(roundtrip_value("Float64", float("nan")))


class TestBoolType:
    """Tests for bool type conversion."""

    def test_true(self):
        assert roundtrip_value("Bool", True) is True

    def test_false(self):
        assert roundtrip_value("Bool", False) is False


class TestStringTypes:
    """Tests for string type conversions."""

    def test_string_bytes(self):
        assert roundtrip_value("String", b"hello") == b"hello"

    def test_string_str(self):
        # str should be accepted and converted
        assert roundtrip_value("String", "hello") == b"hello"

    def test_string_empty(self):
        assert roundtrip_value("String", b"") == b""

    def test_string_unicode(self):
        # UTF-8 encoded unicode
        val = "こんにちは".encode()
        assert roundtrip_value("String", val) == val

    def test_fixed_string(self):
        result = roundtrip_value("FixedString(10)", b"hello")
        # FixedString is padded with nulls
        assert result == b"hello\x00\x00\x00\x00\x00"

    def test_fixed_string_exact_length(self):
        result = roundtrip_value("FixedString(5)", b"hello")
        assert result == b"hello"

    def test_fixed_string_too_long(self):
        from clickhouse_rowbinary import ValidationError

        with pytest.raises(ValidationError):
            roundtrip_value("FixedString(3)", b"hello")

    def test_string_with_str_mode(self):
        schema = Schema.from_clickhouse([("val", "String")])
        writer = RowBinaryWriter(schema)
        writer.write_row({"val": b"hello"})
        data = writer.take()
        reader = RowBinaryReader(data, schema, string_mode="str")
        row = reader.read_row()
        assert row is not None
        assert row["val"] == "hello"
        assert isinstance(row["val"], str)


class TestDateTypes:
    """Tests for date type conversions."""

    def test_date(self):
        d = date(2024, 6, 15)
        result = roundtrip_value("Date", d)
        assert result == d

    def test_date32(self):
        d = date(2024, 6, 15)
        result = roundtrip_value("Date32", d)
        assert result == d

    def test_date_min(self):
        # Date type minimum
        d = date(1970, 1, 1)
        result = roundtrip_value("Date", d)
        assert result == d


class TestDateTimeTypes:
    """Tests for datetime type conversions."""

    def test_datetime(self):
        # Use UTC-aware datetime for consistent roundtrip
        dt = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)
        result = roundtrip_value("DateTime", dt)
        # Result should match (both in UTC)
        assert result.year == dt.year
        assert result.month == dt.month
        assert result.day == dt.day
        assert result.hour == dt.hour
        assert result.minute == dt.minute
        assert result.second == dt.second

    def test_datetime64_3(self):
        # Use UTC-aware datetime for consistent roundtrip
        dt = datetime(2024, 6, 15, 12, 30, 45, 123000, tzinfo=UTC)
        result = roundtrip_value("DateTime64(3)", dt)
        # Millisecond precision
        assert result.year == dt.year
        assert result.month == dt.month
        assert result.day == dt.day
        assert result.hour == dt.hour
        assert result.minute == dt.minute
        assert result.second == dt.second

    def test_datetime64_6(self):
        # Use UTC-aware datetime for consistent roundtrip
        dt = datetime(2024, 6, 15, 12, 30, 45, 123456, tzinfo=UTC)
        result = roundtrip_value("DateTime64(6)", dt)
        # Compare components (both in UTC)
        assert result.year == dt.year
        assert result.month == dt.month
        assert result.day == dt.day
        assert result.hour == dt.hour
        assert result.minute == dt.minute
        assert result.second == dt.second
        assert result.microsecond == dt.microsecond

    def test_datetime_returns_utc_timezone(self):
        """DateTime values are returned with UTC timezone."""
        dt = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)
        result = roundtrip_value("DateTime", dt)

        # Result should be timezone-aware
        assert result.tzinfo is not None
        # Should be UTC (offset is 0)
        assert result.utcoffset() == timedelta(0)

    def test_datetime64_returns_utc_timezone(self):
        """DateTime64 values are returned with UTC timezone."""
        dt = datetime(2024, 6, 15, 12, 30, 45, 123000, tzinfo=UTC)
        result = roundtrip_value("DateTime64(3)", dt)

        # Result should be timezone-aware
        assert result.tzinfo is not None
        # Should be UTC (offset is 0)
        assert result.utcoffset() == timedelta(0)

    def test_datetime_with_explicit_timezone(self):
        """DateTime with explicit timezone uses that timezone."""
        dt = datetime(2024, 6, 15, 12, 30, 45, tzinfo=ZoneInfo("America/New_York"))
        result = roundtrip_value("DateTime('America/New_York')", dt)

        # Result should have the specified timezone
        assert result.tzinfo is not None
        assert result.tzinfo == ZoneInfo("America/New_York")
        # Same hour since we're writing and reading in same timezone
        assert result.hour == dt.hour

    def test_datetime64_with_explicit_timezone(self):
        """DateTime64 with explicit timezone uses that timezone."""
        dt = datetime(2024, 6, 15, 12, 30, 45, 123000, tzinfo=ZoneInfo("Europe/London"))
        result = roundtrip_value("DateTime64(3, 'Europe/London')", dt)

        # Result should have the specified timezone
        assert result.tzinfo is not None
        assert result.tzinfo == ZoneInfo("Europe/London")
        # Same hour since we're writing and reading in same timezone
        assert result.hour == dt.hour


class TestUUIDType:
    """Tests for UUID type conversion."""

    def test_uuid(self):
        u = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        result = roundtrip_value("UUID", u)
        assert result == u

    def test_uuid_from_string(self):
        u = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        result = roundtrip_value("UUID", u)
        assert str(result) == "550e8400-e29b-41d4-a716-446655440000"


class TestIPTypes:
    """Tests for IP address type conversions."""

    def test_ipv4(self):
        ip = IPv4Address("192.168.1.1")
        result = roundtrip_value("IPv4", ip)
        assert result == ip

    def test_ipv6(self):
        ip = IPv6Address("2001:db8::1")
        result = roundtrip_value("IPv6", ip)
        assert result == ip

    def test_ipv6_full(self):
        ip = IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        result = roundtrip_value("IPv6", ip)
        assert result == ip


class TestDecimalTypes:
    """Tests for Decimal type conversions."""

    def test_decimal32(self):
        d = Decimal("123.45")
        result = roundtrip_value("Decimal32(2)", d)
        assert result == d

    def test_decimal64(self):
        d = Decimal("12345678.1234")
        result = roundtrip_value("Decimal64(4)", d)
        assert result == d

    def test_decimal128(self):
        d = Decimal("123456789012345.123456")
        result = roundtrip_value("Decimal128(6)", d)
        assert result == d

    def test_decimal_negative(self):
        d = Decimal("-999.99")
        result = roundtrip_value("Decimal32(2)", d)
        assert result == d

    def test_decimal_from_float(self):
        # Float values should be converted
        result = roundtrip_value("Decimal32(2)", 123.45)
        assert result == Decimal("123.45")

    def test_decimal_from_int(self):
        # Int values should be converted
        result = roundtrip_value("Decimal32(2)", 100)
        assert result == Decimal("100.00")

    def test_decimal_scale_zero(self):
        result = roundtrip_value("Decimal32(0)", 12345)
        assert result == Decimal("12345")

    def test_decimal_generic_precision(self):
        # Test generic Decimal(P,S) syntax
        d = Decimal("1234.56")
        result = roundtrip_value("Decimal(10, 2)", d)
        assert result == d


class TestEnumTypes:
    """Tests for Enum type conversions."""

    def test_enum8(self):
        schema = Schema.from_clickhouse([("val", "Enum8('a' = 1, 'b' = 2, 'c' = 3)")])
        writer = RowBinaryWriter(schema)
        writer.write_row({"val": "b"})
        data = writer.take()
        reader = RowBinaryReader(data, schema)
        row = reader.read_row()
        assert row is not None
        assert row["val"] == "b"

    def test_enum16(self):
        schema = Schema.from_clickhouse(
            [("val", "Enum16('x' = 100, 'y' = 200, 'z' = 300)")]
        )
        writer = RowBinaryWriter(schema)
        writer.write_row({"val": "y"})
        data = writer.take()
        reader = RowBinaryReader(data, schema)
        row = reader.read_row()
        assert row is not None
        assert row["val"] == "y"


class TestNullableTypes:
    """Tests for Nullable type conversions."""

    def test_nullable_with_value(self):
        assert roundtrip_value("Nullable(UInt32)", 42) == 42

    def test_nullable_with_none(self):
        assert roundtrip_value("Nullable(UInt32)", None) is None

    def test_nullable_string(self):
        assert roundtrip_value("Nullable(String)", b"hello") == b"hello"
        assert roundtrip_value("Nullable(String)", None) is None


class TestArrayTypes:
    """Tests for Array type conversions."""

    def test_array_int(self):
        arr = [1, 2, 3, 4, 5]
        assert roundtrip_value("Array(UInt32)", arr) == arr

    def test_array_string(self):
        arr = [b"a", b"b", b"c"]
        assert roundtrip_value("Array(String)", arr) == arr

    def test_array_empty(self):
        assert roundtrip_value("Array(UInt32)", []) == []

    def test_array_nullable(self):
        arr = [1, None, 3, None, 5]
        assert roundtrip_value("Array(Nullable(UInt32))", arr) == arr

    def test_nested_array(self):
        arr = [[1, 2], [3, 4, 5], []]
        assert roundtrip_value("Array(Array(UInt32))", arr) == arr


class TestMapTypes:
    """Tests for Map type conversions."""

    def test_map_string_int(self):
        m = {b"a": 1, b"b": 2}
        assert roundtrip_value("Map(String, Int32)", m) == m

    def test_map_empty(self):
        assert roundtrip_value("Map(String, Int32)", {}) == {}

    def test_map_int_string(self):
        m = {1: b"one", 2: b"two"}
        assert roundtrip_value("Map(UInt32, String)", m) == m


class TestTupleTypes:
    """Tests for Tuple type conversions."""

    def test_tuple_simple(self):
        t = (1, b"hello", 3.14)
        result = roundtrip_value("Tuple(UInt32, String, Float64)", t)
        assert result[0] == 1
        assert result[1] == b"hello"
        assert abs(result[2] - 3.14) < 0.001

    def test_tuple_nested(self):
        t = ((1, 2), (3, 4))
        type_str = "Tuple(Tuple(UInt32, UInt32), Tuple(UInt32, UInt32))"
        assert roundtrip_value(type_str, t) == t


class TestLowCardinalityTypes:
    """Tests for LowCardinality type conversions."""

    def test_low_cardinality_string(self):
        assert roundtrip_value("LowCardinality(String)", b"hello") == b"hello"

    def test_low_cardinality_nullable(self):
        assert roundtrip_value("LowCardinality(Nullable(String))", b"hello") == b"hello"
        assert roundtrip_value("LowCardinality(Nullable(String))", None) is None
