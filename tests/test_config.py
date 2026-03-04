"""Tests for Hydra config composition from real YAML files."""

from pathlib import Path

from hydra import compose, initialize_config_dir
from omegaconf import DictConfig, OmegaConf

CONF_DIR = str(
    Path(__file__).resolve().parent.parent / "src" / "redis_stream_player" / "conf"
)


def _compose(config_name: str, overrides: list[str] | None = None) -> DictConfig:
    with initialize_config_dir(config_dir=CONF_DIR, version_base=None):
        return compose(config_name=config_name, overrides=overrides or [])


class TestConfigComposition:
    """Verify all configs compose without error and have expected keys."""

    def test_record_composes(self):
        cfg = _compose("record")
        assert "redis" in cfg
        assert "streams" in cfg
        assert "output" in cfg
        assert "batch_size" in cfg

    def test_play_composes(self):
        cfg = _compose("play")
        assert "redis" in cfg
        assert "streams" in cfg
        assert "input" in cfg
        assert "speed" in cfg
        assert "max_delay" in cfg

    def test_convert_composes(self):
        cfg = _compose("convert")
        assert "input" in cfg
        assert "output" in cfg
        assert "format" in cfg

    def test_truncate_composes(self):
        cfg = _compose("truncate")
        assert "input" in cfg
        assert "output" in cfg
        assert "from_id" in cfg
        assert "to_id" in cfg

    def test_info_composes(self):
        cfg = _compose("info")
        assert "input" in cfg
        assert "verbose" in cfg

    def test_record_resolves_env_vars(self):
        """Env-var interpolation resolves to null by default."""
        cfg = _compose("record")
        resolved = OmegaConf.to_container(cfg, resolve=True)
        assert isinstance(resolved, dict)
        assert resolved["max_duration"] is None
        assert resolved["max_size_mb"] is None


class TestRedisOverrides:
    """Verify redis config group overrides."""

    def test_local_redis(self):
        cfg = _compose("record", overrides=["redis=local"])
        assert cfg.redis.redis.host == "localhost"
        assert cfg.redis.redis.port == 6379

    def test_prod_redis(self):
        cfg = _compose("record", overrides=["redis=prod"])
        assert cfg.redis.redis.host == "prod-redis"
        assert cfg.redis.redis.port == 6379

    def test_play_prod_redis(self):
        cfg = _compose("play", overrides=["redis=prod"])
        assert cfg.redis.redis.host == "prod-redis"


class TestStreamOverrides:
    """Verify streams config group overrides."""

    def test_sensors_streams(self):
        cfg = _compose("record", overrides=["streams=sensors"])
        streams = cfg.streams.streams
        assert len(streams) == 3
        keys = [s["key"] for s in streams]
        assert "sensor:imu" in keys
        assert "sensor:gps" in keys
        assert "sensor:camera" in keys

    def test_events_streams(self):
        cfg = _compose("record", overrides=["streams=events"])
        streams = cfg.streams.streams
        assert len(streams) == 3
        keys = [s["key"] for s in streams]
        assert "events:user" in keys
        assert "events:system" in keys
        assert "events:audit" in keys

    def test_sensors_timestamp_config(self):
        cfg = _compose("play", overrides=["streams=sensors"])
        streams = cfg.streams.streams
        imu = next(s for s in streams if s["key"] == "sensor:imu")
        assert imu["timestamp_field"] == "receive_ts"
        assert imu["timestamp_mode"] == "bypass"
        camera = next(s for s in streams if s["key"] == "sensor:camera")
        assert camera["timestamp_field"] == "ts_nano"
        assert camera["timestamp_mode"] == "shift"


class TestScalarOverrides:
    """Verify scalar field overrides work."""

    def test_play_speed_override(self):
        cfg = _compose("play", overrides=["speed=2.0"])
        assert cfg.speed == 2.0

    def test_record_batch_size_override(self):
        cfg = _compose("record", overrides=["batch_size=500"])
        assert cfg.batch_size == 500

    def test_record_from_beginning_override(self):
        cfg = _compose("record", overrides=["from_beginning=true"])
        assert cfg.from_beginning is True

    def test_play_max_delay_override(self):
        cfg = _compose("play", overrides=["max_delay=10.0"])
        assert cfg.max_delay == 10.0
