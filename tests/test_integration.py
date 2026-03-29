"""Integration tests against a real Redis (Docker Compose, port 6389).

Run with: ``uv run pytest -m integration``

Requires: ``docker compose up -d redis``
"""

from __future__ import annotations

import math
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

if TYPE_CHECKING:
    from collections.abc import Generator

import pytest
import redis as redis_lib

from boomrdbox.io import RecordReader
from boomrdbox.models import (
    ConvertConf,
    InfoConf,
    MessageID,
    PlayConf,
    RecordConf,
    RedisConf,
    StreamsConf,
    TruncateConf,
)
from boomrdbox.player import Player
from boomrdbox.recorder import Recorder
from boomrdbox.tools import Converter, Info, Truncator

_REDIS_HOST = "localhost"
_REDIS_PORT = 6389
_RECORD_DB = 2
_PLAY_DB = 3


@pytest.fixture
def record_redis() -> Generator[redis_lib.Redis[bytes]]:
    """Connect to Docker Redis record DB or skip."""
    client: redis_lib.Redis[bytes] = redis_lib.Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        db=_RECORD_DB,
    )
    try:
        client.ping()
    except (redis_lib.ConnectionError, ConnectionRefusedError, OSError):
        pytest.skip("Docker Redis not available at localhost:6389")
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def play_redis() -> Generator[redis_lib.Redis[bytes]]:
    """Connect to Docker Redis play DB or skip."""
    client: redis_lib.Redis[bytes] = redis_lib.Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        db=_PLAY_DB,
    )
    try:
        client.ping()
    except (redis_lib.ConnectionError, ConnectionRefusedError, OSError):
        pytest.skip("Docker Redis not available at localhost:6389")
    client.flushdb()
    yield client
    client.flushdb()


def _fill_streams(
    client: redis_lib.Redis[bytes],
    duration_ms: int = 5000,
) -> tuple[int, int]:
    """Fill Redis with synthetic sensor data.

    Returns (total_message_count, base_ms).
    """
    base_ms = int(time.time() * 1000) + 1000
    streams = {
        "sensor:imu": 50,
        "sensor:gps": 1000,
    }
    total = 0
    pipe = client.pipeline(transaction=False)
    for stream_name, interval in streams.items():
        offset = 0
        while offset < duration_ms:
            msg_id = f"{base_ms + offset}-0"
            if stream_name == "sensor:imu":
                fields = {
                    "x": str(math.sin(offset / 1000.0)),
                    "y": str(math.cos(offset / 1000.0)),
                    "z": str(9.81),
                }
            else:
                fields = {
                    "lat": str(55.7558 + offset * 0.000001),
                    "lon": str(37.6173),
                }
            pipe.xadd(stream_name, fields, id=msg_id)
            total += 1
            offset += interval
    pipe.execute()
    return total, base_ms


def _record_conf(tmp_path: Path, **overrides: Any) -> RecordConf:
    defaults: dict[str, Any] = {
        "redis": RedisConf(host=_REDIS_HOST, port=_REDIS_PORT, db=_RECORD_DB),
        "streams": StreamsConf(streams=["sensor:imu", "sensor:gps"]),
        "output": str(tmp_path / "recording.msgpack"),
        "from_beginning": True,
        "batch_size": 500,
        "max_duration": 5.0,
        "verbose": False,
    }
    defaults.update(overrides)
    return RecordConf(**defaults)


def _play_conf(input_path: str, **overrides: Any) -> PlayConf:
    defaults: dict[str, Any] = {
        "redis": RedisConf(host=_REDIS_HOST, port=_REDIS_PORT, db=_PLAY_DB),
        "input": input_path,
        "speed": 10000.0,
        "max_delay": 0.1,
        "batch_size": 500,
        "prefetch": 4,
        "verbose": False,
    }
    defaults.update(overrides)
    return PlayConf(**defaults)


@pytest.mark.integration
class TestRecordFromRedis:
    def test_record_captures_messages(
        self,
        record_redis: redis_lib.Redis[bytes],
        tmp_path: Path,
    ):
        expected, _base_ms = _fill_streams(record_redis, duration_ms=5000)

        conf = _record_conf(tmp_path, max_duration=3.0)
        Recorder(conf).run()

        output = Path(conf.output)
        assert output.exists()
        assert output.stat().st_size > 0

        records = list(RecordReader(conf.output))
        assert len(records) == expected
        streams = {r.stream_name for r in records}
        assert "sensor:imu" in streams
        assert "sensor:gps" in streams


@pytest.mark.integration
class TestPlayToRedis:
    @patch(
        "boomrdbox.player.load_allowed_play_hosts",
        return_value=[_REDIS_HOST],
    )
    def test_play_writes_to_redis(
        self,
        _mock_hosts: Any,
        record_redis: redis_lib.Redis[bytes],
        play_redis: redis_lib.Redis[bytes],
        tmp_path: Path,
    ):
        expected, _base_ms = _fill_streams(record_redis, duration_ms=3000)

        rec_conf = _record_conf(tmp_path, max_duration=3.0)
        Recorder(rec_conf).run()

        play_conf = _play_conf(rec_conf.output)
        Player(play_conf).run()

        imu_len = play_redis.xlen("sensor:imu")
        gps_len = play_redis.xlen("sensor:gps")
        assert imu_len + gps_len == expected

        sample = play_redis.xrange("sensor:imu", count=1)
        assert len(sample) == 1
        fields = sample[0][1]
        assert b"x" in fields
        assert b"y" in fields


@pytest.mark.integration
class TestTruncateAndConvert:
    def test_truncate_preserves_range(
        self,
        record_redis: redis_lib.Redis[bytes],
        tmp_path: Path,
    ):
        _total, base_ms = _fill_streams(record_redis, duration_ms=5000)

        rec_conf = _record_conf(tmp_path, max_duration=3.0)
        Recorder(rec_conf).run()

        mid_ms = base_ms + 2000
        trunc_conf = TruncateConf(
            input=rec_conf.output,
            output=str(tmp_path / "truncated.msgpack"),
            from_id=f"{base_ms}-0",
            to_id=f"{mid_ms}-0",
            verbose=False,
        )
        Truncator(trunc_conf).run()

        records = list(RecordReader(trunc_conf.output))
        assert len(records) > 0
        for r in records:
            assert r.message_id >= MessageID(ms=base_ms, seq=0)
            assert r.message_id <= MessageID(ms=mid_ms, seq=0)

    def test_convert_to_parquet(
        self,
        record_redis: redis_lib.Redis[bytes],
        tmp_path: Path,
    ):
        _total, _base_ms = _fill_streams(record_redis, duration_ms=2000)

        rec_conf = _record_conf(tmp_path, max_duration=3.0)
        Recorder(rec_conf).run()

        parquet_path = str(tmp_path / "out.parquet")
        conv_conf = ConvertConf(
            input=rec_conf.output,
            output=parquet_path,
            format="parquet",
            verbose=False,
        )
        Converter(conv_conf).run()

        import pyarrow.parquet as pq

        table = pq.read_table(parquet_path)
        assert table.num_rows > 0
        assert "stream_name" in table.column_names
        assert "message_id" in table.column_names


@pytest.mark.integration
class TestInfoTool:
    def test_info_prints_stats(
        self,
        record_redis: redis_lib.Redis[bytes],
        tmp_path: Path,
        capsys: Any,
    ):
        _total, _base_ms = _fill_streams(record_redis, duration_ms=2000)

        rec_conf = _record_conf(tmp_path, max_duration=3.0)
        Recorder(rec_conf).run()

        info_conf = InfoConf(input=rec_conf.output, verbose=False)
        Info(info_conf).run()

        captured = capsys.readouterr()
        assert "sensor:imu" in captured.out
        assert "sensor:gps" in captured.out
