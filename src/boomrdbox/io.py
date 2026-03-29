"""Infrastructure layer: Redis factory, msgpack streaming read/write."""

from __future__ import annotations

import os
import socket
import subprocess
import time
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


def _find_free_port() -> int:
    """Find an available local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


class SshTunnel:
    """SSH tunnel via subprocess running ``ssh -L``."""

    def __init__(self, proc: subprocess.Popen[bytes], local_port: int) -> None:
        """Initialize with tunnel subprocess and local port."""
        self._proc = proc
        self.local_bind_port = local_port

    def stop(self) -> None:
        """Terminate the SSH tunnel process."""
        self._proc.terminate()
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    """Wait until a local TCP port accepts connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    msg = f"SSH tunnel port 127.0.0.1:{port} not ready after {timeout}s"
    raise TimeoutError(msg)


def create_tunneled_redis(
    conf: ReadInstanceConf,
) -> tuple[redis.Redis[bytes], Any]:
    """Create a Redis client, optionally through an SSH tunnel.

    Returns (redis_client, tunnel_or_none). The caller must close the tunnel
    when done (call ``tunnel.stop()``).
    """
    if conf.ssh_tunnel is not None and conf.ssh_tunnel.ssh_host:
        local_port = _find_free_port()
        remote = f"{conf.redis.host}:{conf.redis.port}"

        cmd = [
            "ssh",
            "-N",
            "-L",
            f"{local_port}:{remote}",
            "-p",
            str(conf.ssh_tunnel.ssh_port),
        ]
        if conf.ssh_tunnel.ssh_key_file:
            cmd.extend(["-i", conf.ssh_tunnel.ssh_key_file])
        if conf.ssh_tunnel.ssh_user:
            cmd.append(f"{conf.ssh_tunnel.ssh_user}@{conf.ssh_tunnel.ssh_host}")
        else:
            cmd.append(conf.ssh_tunnel.ssh_host)

        logger.info(f"Starting SSH tunnel: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        try:
            _wait_for_port(local_port)
        except TimeoutError:
            proc.kill()
            stderr = (proc.stderr.read() if proc.stderr else b"").decode(
                errors="replace",
            )
            msg = (
                f"SSH tunnel failed to start. stderr: {stderr}\n"
                f"Try running manually: {' '.join(cmd)}"
            )
            raise RuntimeError(msg) from None

        tunnel = SshTunnel(proc, local_port)
        logger.info(
            f"SSH tunnel established: localhost:{local_port}"
            f" -> {remote}"
            f" via {conf.ssh_tunnel.ssh_host}:{conf.ssh_tunnel.ssh_port}",
        )
        client = redis.Redis(
            host="127.0.0.1",
            port=local_port,
            db=conf.redis.db,
            password=conf.redis.password,
            decode_responses=False,
        )
        return client, tunnel
    return create_redis(conf.redis), None


def parse_stream_configs(raw_streams: list[Any]) -> list[StreamConfig]:
    """Parse stream configs from Hydra's raw list.

    Supports both short form (plain strings) and dict form with ``key``.
    """
    configs: list[StreamConfig] = []
    for item in raw_streams:
        if isinstance(item, str):
            configs.append(StreamConfig(key=item))
        elif isinstance(item, StreamItemConf):
            configs.append(StreamConfig(key=item.key))
        elif isinstance(item, dict):
            configs.append(StreamConfig(key=item["key"]))
        else:
            msg = f"Unsupported stream config format: {item!r}"
            raise TypeError(msg)
    for cfg in configs:
        logger.debug(f"Parsed stream config: key={cfg.key}")
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
