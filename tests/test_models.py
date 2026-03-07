"""Tests for domain models."""

import pytest

from boomrdbox.models import (
    InvalidMessageIDError,
    MessageID,
    StreamConfig,
    StreamRecord,
    TimestampMode,
)


class TestMessageID:
    def test_parse_full(self):
        mid = MessageID.parse("1709312000000-5")
        assert mid.ms == 1709312000000
        assert mid.seq == 5

    def test_parse_no_seq(self):
        mid = MessageID.parse("1709312000000")
        assert mid.ms == 1709312000000
        assert mid.seq == 0

    def test_parse_bytes(self):
        mid = MessageID.parse(b"1709312000000-1")
        assert mid.ms == 1709312000000
        assert mid.seq == 1

    def test_parse_invalid(self):
        with pytest.raises(InvalidMessageIDError):
            MessageID.parse("not-a-number")

    def test_parse_invalid_seq(self):
        with pytest.raises(InvalidMessageIDError):
            MessageID.parse("1709312000000-abc")

    def test_str(self):
        mid = MessageID(ms=1709312000000, seq=3)
        assert str(mid) == "1709312000000-3"

    def test_ordering(self):
        a = MessageID(ms=100, seq=0)
        b = MessageID(ms=100, seq=1)
        c = MessageID(ms=200, seq=0)
        assert a < b < c
        assert c > b > a

    def test_equality(self):
        a = MessageID(ms=100, seq=0)
        b = MessageID(ms=100, seq=0)
        assert a == b

    def test_frozen(self):
        mid = MessageID(ms=100, seq=0)
        with pytest.raises(AttributeError):
            mid.ms = 200  # type: ignore[misc]


class TestStreamConfig:
    def test_defaults(self):
        sc = StreamConfig(key="test:stream")
        assert sc.timestamp_field is None
        assert sc.timestamp_mode == TimestampMode.BYPASS

    def test_with_shift(self):
        sc = StreamConfig(
            key="test:stream",
            timestamp_field="ts",
            timestamp_mode=TimestampMode.SHIFT,
        )
        assert sc.timestamp_field == "ts"
        assert sc.timestamp_mode == TimestampMode.SHIFT


class TestStreamRecord:
    def test_creation(self):
        mid = MessageID(ms=100, seq=0)
        record = StreamRecord(
            stream_name="test",
            message_id=mid,
            fields={"key": "value"},
        )
        assert record.stream_name == "test"
        assert record.message_id == mid
        assert record.fields == {"key": "value"}


class TestTimestampMode:
    def test_values(self):
        assert TimestampMode.BYPASS.value == "bypass"
        assert TimestampMode.SHIFT.value == "shift"

    def test_from_string(self):
        assert TimestampMode("bypass") == TimestampMode.BYPASS
        assert TimestampMode("shift") == TimestampMode.SHIFT
