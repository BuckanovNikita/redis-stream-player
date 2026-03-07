"""Infrastructure layer: Redis factory, msgpack streaming read/write."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import msgpack
import redis
from loguru import logger

from boomrdbox.models import (
    MessageID,
    RedisConf,
    StreamConfig,
    StreamItemConf,
    StreamRecord,
    TimestampMode,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def create_redis(conf: RedisConf) -> redis.Redis[bytes]:
    """Create a Redis client from configuration."""
    return redis.Redis(
        host=conf.host,
        port=conf.port,
        db=conf.db,
        password=conf.password,
        decode_responses=False,
    )


def parse_stream_configs(raw_streams: list[Any]) -> list[StreamConfig]:
    """Parse stream configs from Hydra's raw list.

    Supports both short form (plain strings) and dict form with
    ``key``, ``timestamp_field``, and ``timestamp_mode``.
    """
    configs: list[StreamConfig] = []
    for item in raw_streams:
        if isinstance(item, str):
            configs.append(StreamConfig(key=item))
        elif isinstance(item, StreamItemConf):
            mode = TimestampMode(item.timestamp_mode or "bypass")
            configs.append(
                StreamConfig(
                    key=item.key,
                    timestamp_field=item.timestamp_field,
                    timestamp_mode=mode,
                )
            )
        elif isinstance(item, dict):
            mode = TimestampMode(item.get("timestamp_mode", "bypass") or "bypass")
            configs.append(
                StreamConfig(
                    key=item["key"],
                    timestamp_field=item.get("timestamp_field"),
                    timestamp_mode=mode,
                )
            )
        else:
            msg = f"Unsupported stream config format: {item!r}"
            raise TypeError(msg)
    for cfg in configs:
        logger.debug(
            "Parsed stream config: key=%s, ts_field=%s, ts_mode=%s",
            cfg.key,
            cfg.timestamp_field,
            cfg.timestamp_mode.value,
        )
    return configs


class RecordWriter:
    """Streaming msgpack writer for recording Redis stream messages.

    Append-only, crash-safe: each record is packed individually and flushed.
    """

    def __init__(self, path: str | Path) -> None:
        """Initialize writer with the given file path."""
        self._path = Path(path)
        self._file = self._path.open("ab")
        self._packer = msgpack.Packer(use_bin_type=True)
        self._count = 0
        logger.debug("RecordWriter opened: %s", self._path)

    @property
    def path(self) -> str:
        """Return the current file path."""
        return str(self._path)

    @property
    def count(self) -> int:
        """Return the number of records written."""
        return self._count

    @property
    def size(self) -> int:
        """Return the current file size in bytes."""
        return self._file.tell()

    def write(self, record: StreamRecord) -> None:
        """Write a single stream record to the file."""
        data = [record.stream_name, str(record.message_id), record.fields]
        packed = self._packer.pack(data)
        self._file.write(packed)
        self._file.flush()
        self._count += 1

    def rotate(self) -> str:
        """Close current file, open a new one with timestamp suffix.

        Returns the new file path.
        """
        logger.debug("Closing %s (%d records)", self._path, self._count)
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()

        stem = self._path.stem
        ext = self._path.suffix
        parent = self._path.parent
        timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
        self._path = parent / f"{stem}_{timestamp}{ext}"

        self._file = self._path.open("ab")
        self._count = 0
        logger.debug("Opened new file: %s", self._path)
        return str(self._path)

    def close(self) -> None:
        """Flush, fsync, and close the file."""
        if not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            logger.debug(
                "RecordWriter closed: %s (%d records)",
                self._path,
                self._count,
            )

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Exit context manager."""
        self.close()


class RecordReader:
    """Streaming msgpack reader for recorded Redis stream files.

    Handles truncated files gracefully (stops at last complete record).
    """

    def __init__(self, path: str | Path) -> None:
        """Initialize reader with the given file path."""
        self._path = Path(path)
        self._bytes_read = 0

    @property
    def file_size(self) -> int:
        """Return the file size in bytes."""
        return self._path.stat().st_size

    @property
    def bytes_read(self) -> int:
        """Return the number of bytes consumed so far."""
        return self._bytes_read

    def __iter__(self) -> Iterator[StreamRecord]:
        """Iterate over all records in the file."""
        yield from self.read()

    def read(self) -> Iterator[StreamRecord]:
        """Read records from the msgpack file.

        Truncated trailing records are silently skipped with a log warning.
        """
        if not self._path.exists():
            logger.warning("File does not exist: %s", self._path)
            return
        if self._path.stat().st_size == 0:
            logger.warning("File is empty: %s", self._path)
            return

        count = 0
        with self._path.open("rb") as f:
            unpacker = msgpack.Unpacker(f, raw=False, max_buffer_size=0)
            try:
                for obj in unpacker:
                    self._bytes_read = unpacker.tell()
                    if not isinstance(obj, list) or len(obj) != 3:
                        logger.warning("Skipping malformed record: %r", obj)
                        continue
                    stream_name, message_id_str, fields = obj
                    mid = MessageID.parse(str(message_id_str))
                    count += 1
                    yield StreamRecord(
                        stream_name=str(stream_name),
                        message_id=mid,
                        fields=fields,
                    )
            except msgpack.UnpackValueError:
                logger.warning(
                    "Truncated file at %s, stopped at last complete record",
                    self._path,
                )
        logger.debug("RecordReader read %d records from %s", count, self._path)
