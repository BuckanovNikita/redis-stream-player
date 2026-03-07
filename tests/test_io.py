"""Tests for IO layer (msgpack read/write, stream config parsing)."""

from pathlib import Path

import pytest

from boomrdbox.io import RecordReader, RecordWriter, parse_stream_configs
from boomrdbox.models import (
    MessageID,
    StreamConfig,
    StreamRecord,
    TimestampMode,
)


class TestRecordWriterReader:
    def test_write_and_read(self, tmp_msgpack: Path):
        records = [
            StreamRecord("stream:a", MessageID(100, 0), {"key": "val1"}),
            StreamRecord("stream:b", MessageID(200, 1), {"key": "val2"}),
        ]

        with RecordWriter(tmp_msgpack) as writer:
            for r in records:
                writer.write(r)
            assert writer.count == 2
            assert writer.size > 0

        reader = RecordReader(tmp_msgpack)
        result = list(reader)
        assert len(result) == 2
        assert result[0].stream_name == "stream:a"
        assert result[0].message_id == MessageID(100, 0)
        assert result[0].fields == {"key": "val1"}
        assert result[1].stream_name == "stream:b"
        assert result[1].message_id == MessageID(200, 1)

    def test_empty_file(self, tmp_msgpack: Path):
        tmp_msgpack.touch()
        reader = RecordReader(tmp_msgpack)
        assert list(reader) == []

    def test_nonexistent_file(self, tmp_path: Path):
        reader = RecordReader(tmp_path / "missing.msgpack")
        assert list(reader) == []

    def test_truncated_file(self, tmp_msgpack: Path):
        record = StreamRecord("stream:a", MessageID(100, 0), {"key": "val"})
        with RecordWriter(tmp_msgpack) as writer:
            writer.write(record)

        # Truncate last few bytes to simulate crash
        data = tmp_msgpack.read_bytes()
        tmp_msgpack.write_bytes(data[:-3])

        reader = RecordReader(tmp_msgpack)
        # Should not raise, may return 0 or partial records
        results = list(reader)
        assert len(results) <= 1

    def test_rotate(self, tmp_msgpack: Path):
        with RecordWriter(tmp_msgpack) as writer:
            writer.write(StreamRecord("s", MessageID(100, 0), {"a": "1"}))
            new_path = writer.rotate()
            assert new_path != str(tmp_msgpack)
            assert writer.count == 0
            writer.write(StreamRecord("s", MessageID(200, 0), {"b": "2"}))
            assert writer.count == 1

    def test_file_size(self, sample_msgpack: Path):
        reader = RecordReader(sample_msgpack)
        assert reader.file_size > 0

    def test_context_manager(self, tmp_msgpack: Path):
        with RecordWriter(tmp_msgpack) as writer:
            writer.write(StreamRecord("s", MessageID(100, 0), {"a": "1"}))
        # File should be closed after context exit
        assert list(RecordReader(tmp_msgpack)) != []


class TestParseStreamConfigs:
    def test_short_form(self):
        result = parse_stream_configs(["stream:a", "stream:b"])
        assert len(result) == 2
        assert result[0] == StreamConfig(key="stream:a")
        assert result[1] == StreamConfig(key="stream:b")

    def test_dict_form(self):
        result = parse_stream_configs(
            [
                {"key": "stream:a", "timestamp_field": "ts", "timestamp_mode": "shift"},
                {"key": "stream:b"},
            ]
        )
        assert len(result) == 2
        assert result[0] == StreamConfig(
            key="stream:a",
            timestamp_field="ts",
            timestamp_mode=TimestampMode.SHIFT,
        )
        assert result[1] == StreamConfig(key="stream:b")

    def test_invalid_type(self):
        with pytest.raises(TypeError):
            parse_stream_configs([42])
