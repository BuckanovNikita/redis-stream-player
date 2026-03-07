"""Tests for the Recorder."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from boomrdbox.io import RecordReader
from boomrdbox.models import RecordConf, RedisConf, StreamsConf
from boomrdbox.recorder import Recorder


def _make_record_conf(tmp_path: Path, **overrides: Any) -> RecordConf:
    stream_list: list[Any] = overrides.pop("streams", ["test:stream"])
    defaults: dict[str, Any] = {
        "redis": RedisConf(),
        "streams": StreamsConf(streams=stream_list),
        "output": str(tmp_path / "recording.msgpack"),
        "from_beginning": True,
        "rotate_key": None,
        "batch_size": 100,
        "max_duration": None,
        "max_size_mb": None,
        "verbose": False,
    }
    defaults.update(overrides)
    return RecordConf(**defaults)


class TestRecorder:
    @patch("boomrdbox.recorder.create_redis")
    def test_basic_recording(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        call_count = 0

        def xread_side_effect(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    (
                        b"test:stream",
                        [
                            (b"1709312000000-0", {b"key": b"value1"}),
                            (b"1709312000100-0", {b"key": b"value2"}),
                        ],
                    ),
                ]
            recorder._running = False  # noqa: SLF001
            return None

        mock_client.xread.side_effect = xread_side_effect

        conf = _make_record_conf(tmp_path)
        recorder = Recorder(conf)
        recorder.run()

        reader = RecordReader(conf.output)
        records = list(reader)
        assert len(records) == 2
        assert records[0].stream_name == "test:stream"
        assert records[0].fields == {"key": "value1"}

    @patch("boomrdbox.recorder.create_redis")
    def test_from_beginning(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        call_count = 0

        def xread_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            streams = kwargs.get("streams", {})
            if call_count == 1:
                assert streams.get("test:stream") == "0-0"
            recorder._running = False  # noqa: SLF001

        mock_client.xread.side_effect = xread_side_effect

        conf = _make_record_conf(tmp_path, from_beginning=True)
        recorder = Recorder(conf)
        recorder.run()

    @patch("boomrdbox.recorder.create_redis")
    def test_max_duration(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client
        mock_client.xread.return_value = None

        conf = _make_record_conf(tmp_path, max_duration=0.01)
        recorder = Recorder(conf)
        recorder.run()

    @patch("boomrdbox.recorder.create_redis")
    def test_no_streams(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        conf = _make_record_conf(tmp_path, streams=[])
        recorder = Recorder(conf)
        recorder.run()

    def test_hydra_config_composes(self, hydra_record_cfg):
        """Verify Hydra-composed record config has required keys."""
        assert "redis" in hydra_record_cfg
        assert "streams" in hydra_record_cfg
        assert "output" in hydra_record_cfg
