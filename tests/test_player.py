"""Tests for the Player."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from redis_stream_player.io import RecordWriter
from redis_stream_player.models import (
    MessageID,
    PlayConf,
    RedisConf,
    StreamRecord,
    StreamsConf,
)
from redis_stream_player.player import Player

_DEFAULT_PLAY_STREAMS: list[Any] = [
    {
        "key": "sensor:imu",
        "timestamp_field": "receive_ts",
        "timestamp_mode": "bypass",
    },
    {
        "key": "sensor:camera",
        "timestamp_field": "ts_nano",
        "timestamp_mode": "shift",
    },
]


def _make_play_conf(input_path: str, **overrides: Any) -> PlayConf:
    stream_list: list[Any] = overrides.pop("streams", _DEFAULT_PLAY_STREAMS)
    defaults: dict[str, Any] = {
        "redis": RedisConf(),
        "streams": StreamsConf(streams=stream_list),
        "input": input_path,
        "speed": 1.0,
        "max_delay": 60.0,
        "batch_size": 1000,
        "verbose": False,
    }
    defaults.update(overrides)
    return PlayConf(**defaults)


class TestPlayer:
    def test_invalid_speed(self, tmp_path: Path):
        path = str(tmp_path / "x.msgpack")
        with pytest.raises(ValueError, match="speed must be positive"):
            Player(_make_play_conf(path, speed=0))

        with pytest.raises(ValueError, match="speed must be positive"):
            Player(_make_play_conf(path, speed=-1.0))

    @patch("redis_stream_player.player.create_redis")
    def test_play_empty_file(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        fpath = tmp_path / "empty.msgpack"
        fpath.touch()

        conf = _make_play_conf(str(fpath))
        player = Player(conf)
        player.run()
        mock_client.xadd.assert_not_called()

    @patch("redis_stream_player.player.create_redis")
    def test_play_records(self, mock_create_redis, sample_msgpack):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        conf = _make_play_conf(str(sample_msgpack), speed=1000.0)
        player = Player(conf)
        player.run()

        assert mock_client.xadd.call_count == 5

    @patch("redis_stream_player.player.create_redis")
    def test_play_sorts_by_message_id(self, mock_create_redis, tmp_path):
        """Records are replayed in MessageID order within batches."""
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        fpath = tmp_path / "unsorted.msgpack"
        records = [
            StreamRecord("b", MessageID(200, 0), {"v": "2"}),
            StreamRecord("a", MessageID(100, 0), {"v": "1"}),
            StreamRecord("a", MessageID(300, 0), {"v": "3"}),
        ]
        with RecordWriter(fpath) as writer:
            for r in records:
                writer.write(r)

        conf = _make_play_conf(str(fpath), speed=1000.0)
        player = Player(conf)
        player.run()

        calls = mock_client.xadd.call_args_list
        assert len(calls) == 3
        assert calls[0].args[0] == "a"  # ms=100
        assert calls[1].args[0] == "b"  # ms=200
        assert calls[2].args[0] == "a"  # ms=300

    @patch("redis_stream_player.player.create_redis")
    @patch("redis_stream_player.player.time")
    def test_timestamp_shift(self, mock_time, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client
        mock_time.monotonic.return_value = 0.0
        mock_time.time_ns.return_value = 1709312000300_000_000_000

        fpath = tmp_path / "shift.msgpack"
        record = StreamRecord(
            "sensor:camera",
            MessageID(1709312000300, 0),
            {"ts_nano": "1709312000300000000", "frame": "1"},
        )
        with RecordWriter(fpath) as writer:
            writer.write(record)

        conf = _make_play_conf(str(fpath), speed=1000.0)
        player = Player(conf)
        player.run()

        call = mock_client.xadd.call_args
        fields = call.args[1]
        assert "ts_nano" in fields

    def test_hydra_config_composes(self, hydra_play_cfg):
        """Verify Hydra-composed play config has required keys."""
        assert "redis" in hydra_play_cfg
        assert "streams" in hydra_play_cfg
        assert "speed" in hydra_play_cfg
