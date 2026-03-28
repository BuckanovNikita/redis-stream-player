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
    ReadInstanceConf,
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


def create_tunneled_redis(
    conf: ReadInstanceConf,
) -> tuple[redis.Redis[bytes], Any]:
    """Create a Redis client, optionally through an SSH tunnel.

    Returns (redis_client, tunnel_or_none). The caller must close the tunnel
    when done (call ``tunnel.stop()``).
    """
    if conf.ssh_tunnel is not None and conf.ssh_tunnel.ssh_host:
        from sshtunnel import SSHTunnelForwarder

        ssh_kwargs: dict[str, Any] = {
            "ssh_username": conf.ssh_tunnel.ssh_user or None,
            "remote_bind_address": (conf.redis.host, conf.redis.port),
        }
        if conf.ssh_tunnel.ssh_key_file:
            ssh_kwargs["ssh_pkey"] = conf.ssh_tunnel.ssh_key_file
        if conf.ssh_tunnel.ssh_password:
            ssh_kwargs["ssh_password"] = conf.ssh_tunnel.ssh_password

        tunnel = SSHTunnelForwarder(
            (conf.ssh_tunnel.ssh_host, conf.ssh_tunnel.ssh_port),
            **ssh_kwargs,
        )
        tunnel.start()
        logger.info(
            f"SSH tunnel established: localhost:{tunnel.local_bind_port}"
            f" -> {conf.redis.host}:{conf.redis.port}"
            f" via {conf.ssh_tunnel.ssh_host}:{conf.ssh_tunnel.ssh_port}",
        )
        client = redis.Redis(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            db=conf.redis.db,
            password=conf.redis.password,
            decode_responses=False,
        )
        return client, tunnel
    return create_redis(conf.redis), None


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
        ts_mode = cfg.timestamp_mode.value
        logger.debug(
            f"Parsed stream config: key={cfg.key},"
            f" ts_field={cfg.timestamp_field},"
            f" ts_mode={ts_mode}",
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
        logger.debug(f"RecordWriter opened: {self._path}")

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
        logger.debug(f"Closing {self._path} ({self._count} records)")
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
        logger.debug(f"Opened new file: {self._path}")
        return str(self._path)

    def close(self) -> None:
        """Flush, fsync, and close the file."""
        if not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            logger.debug(f"RecordWriter closed: {self._path} ({self._count} records)")

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
            logger.warning(f"File does not exist: {self._path}")
            return
        if self._path.stat().st_size == 0:
            logger.warning(f"File is empty: {self._path}")
            return

        count = 0
        with self._path.open("rb") as f:
            unpacker = msgpack.Unpacker(f, raw=False, max_buffer_size=0)
            try:
                for obj in unpacker:
                    self._bytes_read = unpacker.tell()
                    if not isinstance(obj, list) or len(obj) != 3:
                        logger.warning(f"Skipping malformed record: {obj!r}")
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
                    f"Truncated file at {self._path}, stopped at last complete record",
                )
        logger.debug(f"RecordReader read {count} records from {self._path}")
