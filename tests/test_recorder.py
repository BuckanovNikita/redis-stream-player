"""Tests for the Recorder."""

import signal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import redis as redis_lib

from boomrdbox.io import RecordReader, RecordWriter
from boomrdbox.models import ReadInstanceConf, RecordConf, RedisConf, StreamsConf
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

    @patch("boomrdbox.recorder.create_tunneled_redis")
    @patch("boomrdbox.recorder.get_instance")
    def test_record_with_instance(self, mock_get_instance, mock_tunneled, tmp_path):
        mock_client = MagicMock()
        mock_tunnel = MagicMock()
        inst = ReadInstanceConf(
            name="staging",
            redis=RedisConf(host="10.0.0.5", port=6379),
        )
        mock_get_instance.return_value = inst
        mock_tunneled.return_value = (mock_client, mock_tunnel)

        call_count = 0

        def xread_side_effect(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    (
                        b"test:stream",
                        [(b"100-0", {b"k": b"v"})],
                    ),
                ]
            recorder._running = False  # noqa: SLF001
            return None

        mock_client.xread.side_effect = xread_side_effect

        conf = _make_record_conf(tmp_path, instance="staging")
        recorder = Recorder(conf)
        recorder.run()

        mock_get_instance.assert_called_once_with("staging")
        mock_tunneled.assert_called_once_with(inst)
        mock_tunnel.stop.assert_called_once()

    @patch("boomrdbox.recorder.get_instance", return_value=None)
    def test_record_with_unknown_instance_raises(self, _mock_get, tmp_path):
        conf = _make_record_conf(tmp_path, instance="nonexistent")
        recorder = Recorder(conf)
        with pytest.raises(ValueError, match="not found"):
            recorder.run()


class TestRecorderSignalHandling:
    def test_signal_when_not_running(self, tmp_path):
        conf = _make_record_conf(tmp_path)
        recorder = Recorder(conf)
        recorder._running = False  # noqa: SLF001
        with pytest.raises(KeyboardInterrupt):
            recorder._handle_signal(signal.SIGINT, None)  # noqa: SLF001

    def test_signal_when_running(self, tmp_path):
        conf = _make_record_conf(tmp_path)
        recorder = Recorder(conf)
        recorder._running = True  # noqa: SLF001
        recorder._handle_signal(signal.SIGINT, None)  # noqa: SLF001
        assert recorder._running is False  # noqa: SLF001
        assert recorder._stop_event.is_set()  # noqa: SLF001


class TestProcessXreadResult:
    def _make_recorder(self, tmp_path: Path) -> Recorder:
        conf = _make_record_conf(tmp_path)
        return Recorder(conf)

    def test_decode_error_on_stream_name(self, tmp_path):
        recorder = self._make_recorder(tmp_path)
        result = [
            (b"\xff\xfe", [(b"100-0", {b"k": b"v"})]),
        ]
        stream_keys = {"test:stream": None}
        warned: set[str] = set()
        last_ids = {"test:stream": "0-0"}
        with RecordWriter(tmp_path / "out.msgpack") as writer:
            count = recorder._process_xread_result(  # noqa: SLF001
                result,
                stream_keys,
                warned,
                last_ids,
                writer,
            )
        assert count == 0

    def test_unknown_stream_warns_once(self, tmp_path):
        recorder = self._make_recorder(tmp_path)
        result = [
            (b"unknown:stream", [(b"100-0", {b"k": b"v"})]),
        ]
        stream_keys = {"test:stream": None}
        warned: set[str] = set()
        last_ids = {"test:stream": "0-0"}
        with RecordWriter(tmp_path / "out.msgpack") as writer:
            count = recorder._process_xread_result(  # noqa: SLF001
                result,
                stream_keys,
                warned,
                last_ids,
                writer,
            )
        assert count == 0
        assert "unknown:stream" in warned

    def test_msg_id_decode_error(self, tmp_path):
        recorder = self._make_recorder(tmp_path)
        result = [
            (b"test:stream", [(b"\xff\xfe", {b"k": b"v"})]),
        ]
        stream_keys = {"test:stream": None}
        warned: set[str] = set()
        last_ids = {"test:stream": "0-0"}
        with RecordWriter(tmp_path / "out.msgpack") as writer:
            count = recorder._process_xread_result(  # noqa: SLF001
                result,
                stream_keys,
                warned,
                last_ids,
                writer,
            )
        assert count == 0

    def test_field_decode_error(self, tmp_path):
        recorder = self._make_recorder(tmp_path)
        bad_fields = MagicMock()
        bad_fields.items.side_effect = UnicodeDecodeError(
            "utf-8",
            b"",
            0,
            1,
            "bad",
        )
        result: Any = [
            (b"test:stream", [(b"100-0", bad_fields)]),
        ]
        stream_keys = {"test:stream": None}
        warned: set[str] = set()
        last_ids = {"test:stream": "0-0"}
        with RecordWriter(tmp_path / "out.msgpack") as writer:
            count = recorder._process_xread_result(  # noqa: SLF001
                result,
                stream_keys,
                warned,
                last_ids,
                writer,
            )
        assert count == 0


class TestMaybeRotate:
    @patch("boomrdbox.recorder.create_redis")
    def test_rotate_flag_triggers_rotation(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        call_count = 0

        def xread_side_effect(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                recorder._rotate_flag.set()  # noqa: SLF001
                return [
                    (b"test:stream", [(b"100-0", {b"k": b"v"})]),
                ]
            recorder._running = False  # noqa: SLF001
            return None

        mock_client.xread.side_effect = xread_side_effect

        conf = _make_record_conf(tmp_path)
        recorder = Recorder(conf)
        recorder.run()


class TestXreadErrorRecovery:
    @patch("boomrdbox.recorder.create_redis")
    def test_xread_redis_error_continues(self, mock_create_redis, tmp_path):
        mock_client = MagicMock()
        mock_create_redis.return_value = mock_client

        call_count = 0

        def xread_side_effect(**_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise redis_lib.exceptions.RedisError("connection lost")
            recorder._running = False  # noqa: SLF001

        mock_client.xread.side_effect = xread_side_effect

        conf = _make_record_conf(tmp_path)
        recorder = Recorder(conf)
        recorder.run()
