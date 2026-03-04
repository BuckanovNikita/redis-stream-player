"""Shared test fixtures."""

from pathlib import Path

import pytest
from hydra import compose, initialize_config_dir
from omegaconf import DictConfig

from redis_stream_player.io import RecordWriter
from redis_stream_player.models import (
    MessageID,
    StreamRecord,
)

CONF_DIR = str(
    Path(__file__).resolve().parent.parent / "src" / "redis_stream_player" / "conf"
)


@pytest.fixture
def tmp_msgpack(tmp_path: Path) -> Path:
    return tmp_path / "test.msgpack"


@pytest.fixture
def sample_records() -> list[StreamRecord]:
    return [
        StreamRecord(
            stream_name="sensor:imu",
            message_id=MessageID(ms=1709312000000, seq=0),
            fields={
                "x": "1.0",
                "y": "2.0",
                "z": "3.0",
                "receive_ts": "1709312000000000000",
            },
        ),
        StreamRecord(
            stream_name="sensor:gps",
            message_id=MessageID(ms=1709312000100, seq=0),
            fields={"lat": "55.7558", "lon": "37.6173"},
        ),
        StreamRecord(
            stream_name="sensor:imu",
            message_id=MessageID(ms=1709312000200, seq=0),
            fields={
                "x": "1.1",
                "y": "2.1",
                "z": "3.1",
                "receive_ts": "1709312000200000000",
            },
        ),
        StreamRecord(
            stream_name="sensor:camera",
            message_id=MessageID(ms=1709312000300, seq=0),
            fields={
                "frame": "42",
                "ts_nano": "1709312000300000000",
            },
        ),
        StreamRecord(
            stream_name="sensor:imu",
            message_id=MessageID(ms=1709312000500, seq=0),
            fields={
                "x": "1.2",
                "y": "2.2",
                "z": "3.2",
                "receive_ts": "1709312000500000000",
            },
        ),
    ]


@pytest.fixture
def sample_msgpack(tmp_msgpack: Path, sample_records: list[StreamRecord]) -> Path:
    with RecordWriter(tmp_msgpack) as writer:
        for record in sample_records:
            writer.write(record)
    return tmp_msgpack


def _compose_config(config_name: str, overrides: list[str] | None = None) -> DictConfig:
    """Compose a Hydra config from real YAML files."""
    with initialize_config_dir(config_dir=CONF_DIR, version_base=None):
        return compose(config_name=config_name, overrides=overrides or [])


@pytest.fixture
def hydra_record_cfg() -> DictConfig:
    return _compose_config("record")


@pytest.fixture
def hydra_play_cfg() -> DictConfig:
    return _compose_config("play")


@pytest.fixture
def hydra_convert_cfg() -> DictConfig:
    return _compose_config("convert")


@pytest.fixture
def hydra_truncate_cfg() -> DictConfig:
    return _compose_config("truncate")


@pytest.fixture
def hydra_info_cfg() -> DictConfig:
    return _compose_config("info")
