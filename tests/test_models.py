"""Tests for domain models."""

import pytest
from pydantic import ValidationError

from boomrdbox.models import (
    ConvertConf,
    InvalidMessageIDError,
    MessageID,
    RecordConf,
    RedisConf,
    StreamConfig,
    StreamItemConf,
    StreamRecord,
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
        assert sc.key == "test:stream"


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


class TestConfigValidation:
    def test_redis_port_too_low(self):
        with pytest.raises(ValidationError):
            RedisConf(port=0)

    def test_redis_port_too_high(self):
        with pytest.raises(ValidationError):
            RedisConf(port=70000)

    def test_record_batch_size_zero(self):
        with pytest.raises(ValidationError):
            RecordConf(batch_size=0)

    def test_convert_invalid_format(self):
        with pytest.raises(ValidationError):
            ConvertConf(format="json")

    def test_valid_defaults(self):
        RedisConf()
        RecordConf()
        StreamItemConf()
        ConvertConf()
