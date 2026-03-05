"""Domain models for redis-stream-player.

This module contains pure domain types with no third-party dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TimestampMode(Enum):
    """How to handle timestamp fields during playback."""

    BYPASS = "bypass"
    SHIFT = "shift"


class InvalidMessageIDError(ValueError):
    """Raised when a Redis message ID string cannot be parsed."""


@dataclass(frozen=True, order=True)
class MessageID:
    """Parsed Redis message ID with ordering support.

    Redis message IDs have the format ``<ms_timestamp>-<sequence>``.
    """

    ms: int
    seq: int = 0

    @classmethod
    def parse(cls, raw: str | bytes) -> MessageID:
        """Parse a Redis message ID string like ``"1709312000000-0"``.

        Also accepts ``"1709312000000"`` (seq defaults to 0).
        """
        text = raw.decode() if isinstance(raw, bytes) else raw
        parts = text.split("-", maxsplit=1)
        try:
            ms = int(parts[0])
        except (ValueError, IndexError) as exc:
            msg = f"Invalid message ID: {text!r}"
            raise InvalidMessageIDError(msg) from exc
        seq = 0
        if len(parts) == 2:
            try:
                seq = int(parts[1])
            except ValueError as exc:
                msg = f"Invalid message ID sequence: {text!r}"
                raise InvalidMessageIDError(msg) from exc
        return cls(ms=ms, seq=seq)

    def __str__(self) -> str:
        """Return the string representation of the message ID."""
        return f"{self.ms}-{self.seq}"


@dataclass(frozen=True)
class StreamConfig:
    """Configuration for a single Redis stream to record/play."""

    key: str
    timestamp_field: str | None = None
    timestamp_mode: TimestampMode = TimestampMode.BYPASS


@dataclass(frozen=True)
class StreamRecord:
    """A single recorded message from a Redis stream."""

    stream_name: str
    message_id: MessageID
    fields: dict[str, Any]


# ---------------------------------------------------------------------------
# Hydra structured config dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RedisConf:
    """Redis connection configuration."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None


@dataclass
class StreamItemConf:
    """Single stream item in Hydra config."""

    key: str = ""
    timestamp_field: str | None = None
    timestamp_mode: str = "bypass"


@dataclass
class StreamsConf:
    """Streams list configuration."""

    streams: list[Any] = field(default_factory=list)


@dataclass
class RecordConf:
    """Recorder Hydra configuration."""

    redis: RedisConf = field(default_factory=RedisConf)
    streams: StreamsConf = field(default_factory=StreamsConf)
    output: str = "recording.msgpack"
    from_beginning: bool = False
    rotate_key: str | None = None
    batch_size: int = 100
    max_duration: float | None = None
    max_size_mb: float | None = None
    verbose: bool = False


@dataclass
class PlayConf:
    """Player Hydra configuration."""

    redis: RedisConf = field(default_factory=RedisConf)
    streams: StreamsConf = field(default_factory=StreamsConf)
    input: str = "recording.msgpack"
    speed: float = 1.0
    max_delay: float = 60.0
    batch_size: int = 1000
    prefetch: int = 4
    verbose: bool = False


@dataclass
class ConvertConf:
    """Converter Hydra configuration."""

    input: str = "recording.msgpack"
    output: str = "recording.parquet"
    format: str = "parquet"
    verbose: bool = False


@dataclass
class TruncateConf:
    """Truncator Hydra configuration."""

    input: str = "recording.msgpack"
    output: str = "truncated.msgpack"
    from_id: str | None = None
    to_id: str | None = None
    auto_start: bool = False
    verbose: bool = False


@dataclass
class InfoConf:
    """Info tool Hydra configuration."""

    input: str = "recording.msgpack"
    verbose: bool = False
