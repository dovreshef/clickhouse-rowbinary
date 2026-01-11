"""Tests for SeekableReader and SeekableWriter (Zstd-compressed files)."""

from pathlib import Path

import pytest
from clickhouse_rowbinary import (
    Format,
    RowBinaryWriter,
    Schema,
    SeekableReader,
    SeekableWriter,
)


@pytest.fixture
def tmp_zst_file(tmp_path: Path) -> Path:
    """Temporary file path for Zstd files."""
    return tmp_path / "test.rowbinary.zst"


class TestSeekableWriter:
    """Tests for SeekableWriter."""

    def test_create_and_write(self, simple_schema: Schema, tmp_zst_file: Path) -> None:
        """Write rows to compressed file."""
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            writer.write_row({"id": 1, "name": b"Alice", "active": True})
            writer.write_row({"id": 2, "name": b"Bob", "active": False})

        assert tmp_zst_file.exists()
        assert tmp_zst_file.stat().st_size > 0

    def test_rows_written_count(
        self, simple_schema: Schema, tmp_zst_file: Path
    ) -> None:
        """Track rows written count."""
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            assert writer.rows_written == 0
            writer.write_row({"id": 1, "name": b"Alice", "active": True})
            assert writer.rows_written == 1
            writer.write_row({"id": 2, "name": b"Bob", "active": False})
            assert writer.rows_written == 2

    def test_write_rows_batch(self, simple_schema: Schema, tmp_zst_file: Path) -> None:
        """Write multiple rows at once."""
        rows = [
            {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
            for i in range(100)
        ]
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            writer.write_rows(rows)
            assert writer.rows_written == 100

    def test_context_manager_calls_finish(
        self, simple_schema: Schema, tmp_zst_file: Path
    ) -> None:
        """Context manager finalizes file."""
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            writer.write_row({"id": 1, "name": b"test", "active": True})
        # File should be finalized and readable
        assert tmp_zst_file.exists()

    def test_write_row_bytes(self, simple_schema: Schema, tmp_zst_file: Path) -> None:
        """Write pre-encoded bytes."""
        # First, encode a row using RowBinaryWriter
        plain_writer = RowBinaryWriter(simple_schema)
        plain_writer.write_row({"id": 42, "name": b"test", "active": True})
        row_bytes = plain_writer.take()

        # Write the raw bytes to compressed file
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            writer.write_row_bytes(row_bytes)
            assert writer.rows_written == 1


class TestSeekableReader:
    """Tests for SeekableReader."""

    @pytest.fixture
    def sample_zst_file(self, simple_schema: Schema, tmp_path: Path) -> Path:
        """Create a sample compressed file with 100 rows."""
        path = tmp_path / "sample.rowbinary.zst"
        with SeekableWriter.create(path, simple_schema) as writer:
            writer.write_header()
            for i in range(100):
                writer.write_row(
                    {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
                )
        return path

    def test_open_and_read(self, simple_schema: Schema, sample_zst_file: Path) -> None:
        """Read rows from compressed file."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            row = reader.read_current()
            assert row is not None
            assert row["id"] == 0
            assert row["name"] == b"user0"

    def test_iteration(self, simple_schema: Schema, sample_zst_file: Path) -> None:
        """Iterator protocol works."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            rows = list(reader)
            assert len(rows) == 100
            assert rows[0]["id"] == 0
            assert rows[99]["id"] == 99

    def test_seek_absolute(self, simple_schema: Schema, sample_zst_file: Path) -> None:
        """Seek to specific row index."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            reader.seek(50)
            row = reader.read_current()
            assert row is not None
            assert row["id"] == 50
            assert row["name"] == b"user50"

    def test_seek_relative(self, simple_schema: Schema, sample_zst_file: Path) -> None:
        """Seek forward and backward."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Start at 0, peek without advancing
            row = reader.read_current(advance=False)
            assert row is not None
            assert row["id"] == 0

            # Move forward 10
            reader.seek_relative(10)
            row = reader.read_current(advance=False)
            assert row is not None
            assert row["id"] == 10

            # Move backward 5
            reader.seek_relative(-5)
            row = reader.read_current(advance=False)
            assert row is not None
            assert row["id"] == 5

    def test_current_row_bytes(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """Get raw bytes without decoding."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            row_bytes = reader.current_row_bytes()
            assert row_bytes is not None
            assert isinstance(row_bytes, bytes)
            assert len(row_bytes) > 0

    def test_read_rows_batch(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """Batch reading works."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            rows = reader.read_rows(10)
            assert len(rows) == 10
            for i, row in enumerate(rows):
                assert row["id"] == i

    def test_schema_property(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """Schema property returns correct schema."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            assert len(reader.schema.columns) == 3
            assert reader.schema.names == ["id", "name", "active"]

    def test_current_index(self, simple_schema: Schema, sample_zst_file: Path) -> None:
        """current_index returns the correct row position."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Initial position is 0
            assert reader.current_index == 0

            # After seek, position updates
            reader.seek(50)
            assert reader.current_index == 50

            # After relative seek forward
            reader.seek_relative(10)
            assert reader.current_index == 60

            # After relative seek backward
            reader.seek_relative(-5)
            assert reader.current_index == 55

            # After seek_to_start
            reader.seek_to_start()
            assert reader.current_index == 0

    def test_read_current_advance_true(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """read_current with advance=True (default) advances position."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Read with default advance=True
            row0 = reader.read_current()
            assert row0 is not None
            assert row0["id"] == 0
            assert reader.current_index == 1  # Advanced

            row1 = reader.read_current()
            assert row1 is not None
            assert row1["id"] == 1
            assert reader.current_index == 2  # Advanced again

    def test_read_current_advance_false(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """read_current with advance=False keeps position unchanged."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Read with advance=False
            row0 = reader.read_current(advance=False)
            assert row0 is not None
            assert row0["id"] == 0
            assert reader.current_index == 0  # Not advanced

            # Reading again returns the same row
            row0_again = reader.read_current(advance=False)
            assert row0_again is not None
            assert row0_again["id"] == 0
            assert reader.current_index == 0  # Still not advanced

            # Now advance manually and read again
            row0_final = reader.read_current(advance=True)
            assert row0_final["id"] == 0
            assert reader.current_index == 1  # Now advanced

    def test_read_current_advance_mixed(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """Mix of advance=True and advance=False works correctly."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            reader.seek(10)

            # Peek at row 10
            row = reader.read_current(advance=False)
            assert row["id"] == 10
            assert reader.current_index == 10

            # Peek again
            row = reader.read_current(advance=False)
            assert row["id"] == 10
            assert reader.current_index == 10

            # Now consume
            row = reader.read_current(advance=True)
            assert row["id"] == 10
            assert reader.current_index == 11

            # Next read gets row 11
            row = reader.read_current(advance=True)
            assert row["id"] == 11
            assert reader.current_index == 12

    def test_read_current_at_end_of_file(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """read_current at end of file returns None."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Seek to last row (99, since we have 100 rows 0-99)
            reader.seek(99)
            assert reader.current_index == 99

            # Read last row with advance=True
            row = reader.read_current(advance=True)
            assert row is not None
            assert row["id"] == 99

            # Now at position 100 (past end)
            # Reading should return None
            row = reader.read_current(advance=True)
            assert row is None

            # Reading again should still return None
            row = reader.read_current(advance=False)
            assert row is None

    def test_read_current_at_end_no_advance(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """read_current with advance=False at last row works correctly."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            # Seek to last row
            reader.seek(99)

            # Peek at last row without advancing
            row = reader.read_current(advance=False)
            assert row is not None
            assert row["id"] == 99
            assert reader.current_index == 99

            # Peek again - should still work
            row = reader.read_current(advance=False)
            assert row is not None
            assert row["id"] == 99
            assert reader.current_index == 99

            # Now advance past end
            row = reader.read_current(advance=True)
            assert row is not None
            assert row["id"] == 99

            # Subsequent reads should return None
            row = reader.read_current(advance=False)
            assert row is None

    def test_iteration_uses_advance(
        self, simple_schema: Schema, sample_zst_file: Path
    ) -> None:
        """Iterator properly advances through all rows."""
        with SeekableReader.open(sample_zst_file, schema=simple_schema) as reader:
            rows = []
            for row in reader:
                rows.append(row["id"])

            # Should have read all 100 rows
            assert len(rows) == 100
            assert rows == list(range(100))

            # After iteration, further reads return None
            row = reader.read_current(advance=False)
            assert row is None


class TestRoundtrip:
    """Roundtrip tests between SeekableWriter and SeekableReader."""

    def test_write_read_roundtrip(
        self, simple_schema: Schema, tmp_zst_file: Path
    ) -> None:
        """Write then read produces same data."""
        original_rows = [
            {"id": 1, "name": b"Alice", "active": True},
            {"id": 2, "name": b"Bob", "active": False},
            {"id": 3, "name": b"Charlie", "active": True},
        ]

        # Write
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            writer.write_rows(original_rows)

        # Read back
        with SeekableReader.open(tmp_zst_file, schema=simple_schema) as reader:
            read_rows = list(reader)

        assert len(read_rows) == len(original_rows)
        for orig, read in zip(original_rows, read_rows, strict=True):
            assert read["id"] == orig["id"]
            assert read["name"] == orig["name"]
            assert read["active"] == orig["active"]

    def test_large_file(self, simple_schema: Schema, tmp_zst_file: Path) -> None:
        """Handle 10k+ rows efficiently."""
        row_count = 10000

        # Write
        with SeekableWriter.create(tmp_zst_file, simple_schema) as writer:
            writer.write_header()
            for i in range(row_count):
                writer.write_row(
                    {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
                )

        # Read and verify random access
        with SeekableReader.open(tmp_zst_file, schema=simple_schema) as reader:
            # Check first row
            row = reader.read_current()
            assert row is not None
            assert row["id"] == 0

            # Jump to middle
            reader.seek(5000)
            row = reader.read_current()
            assert row is not None
            assert row["id"] == 5000

            # Jump to end
            reader.seek(9999)
            row = reader.read_current()
            assert row is not None
            assert row["id"] == 9999


class TestBatchProcessing:
    """Tests for high-performance batch processing patterns."""

    def test_raw_bytes_passthrough(self, simple_schema: Schema, tmp_path: Path) -> None:
        """Raw bytes can pass through without decode/re-encode."""
        input_file = tmp_path / "input.rowbinary.zst"
        output_file = tmp_path / "output.rowbinary.zst"

        # Create input file
        with SeekableWriter.create(input_file, simple_schema) as writer:
            writer.write_header()
            for i in range(50):
                writer.write_row(
                    {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
                )

        # Copy using raw bytes (no decoding)
        with (
            SeekableReader.open(input_file, schema=simple_schema) as reader,
            SeekableWriter.create(output_file, simple_schema) as writer,
        ):
            writer.write_header()
            while (row_bytes := reader.current_row_bytes()) is not None:
                writer.write_row_bytes(row_bytes)
                # Try to advance, but handle end of file
                try:
                    reader.seek_relative(1)
                except Exception:
                    break  # End of file

        # Verify output
        with SeekableReader.open(output_file, schema=simple_schema) as reader:
            rows = list(reader)
            assert len(rows) == 50
            assert rows[0]["id"] == 0
            assert rows[49]["id"] == 49

    def test_rebatch_to_plain_format(
        self, simple_schema: Schema, tmp_path: Path
    ) -> None:
        """Read compressed, rebatch to plain format."""
        zst_file = tmp_path / "compressed.rowbinary.zst"

        # Create compressed file
        with SeekableWriter.create(zst_file, simple_schema) as writer:
            writer.write_header()
            for i in range(100):
                writer.write_row(
                    {"id": i, "name": f"user{i}".encode(), "active": i % 2 == 0}
                )

        # Read compressed, create batches of plain RowBinary
        batch_size = 25
        batches: list[bytes] = []

        with SeekableReader.open(zst_file, schema=simple_schema) as reader:
            while True:
                batch_writer = RowBinaryWriter(
                    simple_schema, format=Format.RowBinaryWithNamesAndTypes
                )
                batch_writer.write_header()

                count = 0
                for _ in range(batch_size):
                    row_bytes = reader.current_row_bytes()
                    if row_bytes is None:
                        break
                    batch_writer.write_row_bytes(row_bytes)
                    count += 1
                    # Try to advance, but handle end of file
                    try:
                        reader.seek_relative(1)
                    except Exception:
                        break  # End of file

                if count == 0:
                    break

                batches.append(batch_writer.take())

        assert len(batches) == 4  # 100 rows / 25 per batch

        # Verify each batch can be decoded
        total_rows = 0
        for batch in batches:
            from clickhouse_rowbinary import RowBinaryReader

            reader = RowBinaryReader(
                batch, simple_schema, format=Format.RowBinaryWithNamesAndTypes
            )
            rows = reader.read_all()
            total_rows += len(rows)

        assert total_rows == 100
