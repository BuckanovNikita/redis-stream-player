"""Domain models for boomrdbox.

This module contains pure domain types with no third-party dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass


class InvalidMessageIDError(ValueError):
    """Raised when a Redis message ID string cannot be parsed."""


class UnsafePlayTargetError(RuntimeError):
    """Raised when the play command targets a non-whitelisted Redis host."""


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


@dataclass(frozen=True)
class StreamRecord:
    """A single recorded message from a Redis stream."""

    stream_name: str
    message_id: MessageID
    fields: dict[str, Any]


# ---------------------------------------------------------------------------
# Hydra structured config dataclasses
# ---------------------------------------------------------------------------


@pydantic_dataclass
class RedisConf:
    """Redis connection configuration."""

    host: str = "localhost"
    port: Annotated[int, Field(ge=1, le=65535)] = 6379
    db: Annotated[int, Field(ge=0)] = 0
    password: str | None = None


@pydantic_dataclass
class SshTunnelConf:
    """SSH tunnel configuration for reaching remote Redis."""

    ssh_host: str = ""
    ssh_port: Annotated[int, Field(ge=1, le=65535)] = 22
    ssh_user: str = ""
    ssh_key_file: str | None = None
    ssh_password: str | None = None


@pydantic_dataclass
class ReadInstanceConf:
    """A named Redis read-instance with optional SSH tunnel.

    Used by record command for reading from remote Redis.
    Must NEVER be used by the play command.
    """

    name: str = ""
    redis: RedisConf = field(default_factory=RedisConf)
    ssh_tunnel: SshTunnelConf | None = None


_VALID_CONVERT_FORMATS = frozenset({"parquet", "csv"})


@pydantic_dataclass
class StreamItemConf:
    """Single stream item in Hydra config."""

    key: str = ""


@pydantic_dataclass
class StreamsConf:
    """Streams list configuration."""

    streams: list[Any] = field(default_factory=list)


@pydantic_dataclass
class RecordConf:
    """Recorder Hydra configuration."""

    redis: RedisConf = field(default_factory=RedisConf)
    streams: StreamsConf = field(default_factory=StreamsConf)
    output: str = "recording.msgpack"
    instance: str | None = None
    from_beginning: bool = False
    rotate_key: str | None = None
    batch_size: Annotated[int, Field(gt=0)] = 100
    max_duration: Annotated[float | None, Field(gt=0)] = None
    max_size_mb: Annotated[float | None, Field(gt=0)] = None
    verbose: bool = False


@pydantic_dataclass
class PlayConf:
    """Player Hydra configuration."""

    redis: RedisConf = field(default_factory=RedisConf)
    input: str = "recording.msgpack"
    speed: Annotated[float, Field(gt=0)] = 1.0
    max_delay: Annotated[float, Field(gt=0)] = 60.0
    batch_size: Annotated[int, Field(gt=0)] = 1000
    prefetch: Annotated[int, Field(gt=0)] = 4
    verbose: bool = False
    exclude_streams: list[str] = field(default_factory=list)
    rename_streams: dict[str, str] = field(default_factory=dict)


@pydantic_dataclass
class ConvertConf:
    """Converter Hydra configuration."""

    input: str = "recording.msgpack"
    output: str = "recording.parquet"
    format: str = "parquet"
    verbose: bool = False

    @field_validator("format")
    @classmethod
    def _check_format(cls, v: str) -> str:
        if v not in _VALID_CONVERT_FORMATS:
            msg = f"format must be one of {sorted(_VALID_CONVERT_FORMATS)}, got {v!r}"
            raise ValueError(msg)
        return v


@pydantic_dataclass
class TruncateConf:
    """Truncator Hydra configuration."""

    input: str = "recording.msgpack"
    output: str = "truncated.msgpack"
    from_id: str | None = None
    to_id: str | None = None
    auto_start: bool = False
    interactive: bool = False
    verbose: bool = False


@pydantic_dataclass
class InfoConf:
    """Info tool Hydra configuration."""

    input: str = "recording.msgpack"
    verbose: bool = False
