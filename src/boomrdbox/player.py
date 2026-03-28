"""Application layer: Redis stream player."""

from __future__ import annotations

import queue
import signal
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgpack
from loguru import logger
from tqdm import tqdm

from boomrdbox.instances import load_allowed_play_hosts
from boomrdbox.io import (
    RecordReader,
    create_redis,
)
from boomrdbox.models import (
    MessageID,
    PlayConf,
    StreamRecord,
    UnsafePlayTargetError,
)

if TYPE_CHECKING:
    from types import FrameType

    import redis as redis_lib


def validate_play_target(host: str, allowed_hosts: list[str]) -> None:
    """Raise UnsafePlayTargetError if the host is not in the whitelist."""
    if host not in allowed_hosts:
        allowed = ", ".join(repr(h) for h in allowed_hosts)
        msg = (
            f"Play command refused: host {host!r} is not in the allowed"
            f" hosts list [{allowed}]. Configure allowed hosts via"
            f" 'boomrdbox setup play-hosts add <hostname>'."
        )
        raise UnsafePlayTargetError(msg)


def _format_ms(ms: int) -> str:
    total_seconds = ms // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


@dataclass(frozen=True)
class _BatchMeta:
    """Metadata about stream time boundaries for progress tracking."""

    first_ms: int
    last_ms: int


@dataclass(frozen=True)
class _PreparedRecord:
    """A record with pre-computed XADD fields ready for replay."""

    stream_name: str
    message_id: MessageID
    xadd_fields: dict[str, str | bytes]


class Player:
    """Replays recorded Redis stream messages."""

    def __init__(self, conf: PlayConf) -> None:
        """Initialize with player configuration."""
        self._conf = conf
        self._exclude_streams: set[str] = set(conf.exclude_streams)
        self._rename_streams: dict[str, str] = dict(conf.rename_streams)
        self._running = False
        self._stop_event = threading.Event()
        self._client: redis_lib.Redis[bytes] | None = None
        self._progress: tqdm[Any] | None = None
        logger.debug(
            f"Player config: input={conf.input},"
            f" speed={conf.speed},"
            f" max_delay={conf.max_delay},"
            f" batch_size={conf.batch_size},"
            f" prefetch={conf.prefetch}",
        )

    def _handle_signal(self, _signum: int, _frame: FrameType | None) -> None:
        if not self._running:
            raise KeyboardInterrupt
        logger.info("Shutdown signal received, stopping...")
        self._running = False
        self._stop_event.set()

    def _prepare_fields(
        self,
        record: StreamRecord,
    ) -> dict[str, str | bytes]:
        """Prepare XADD fields from a record."""
        xadd_fields: dict[str, str | bytes] = {}
        for k, v in record.fields.items():
            key = k if isinstance(k, str) else str(k)
            xadd_fields[key] = v if isinstance(v, (str, bytes)) else str(v)
        return xadd_fields

    def _producer_loop(
        self,
        q: queue.Queue[tuple[list[_PreparedRecord], _BatchMeta] | None],
        reader: RecordReader,
    ) -> None:
        """Read records from file, prepare batches, and enqueue them."""
        batch_size = self._conf.batch_size
        try:
            batch: list[StreamRecord] = []
            first_ms: int | None = None
            last_ms = 0
            for record in reader:
                if self._stop_event.is_set():
                    break
                if record.stream_name in self._exclude_streams:
                    continue
                batch.append(record)

                if len(batch) >= batch_size:
                    batch.sort(key=lambda r: r.message_id)
                    prepared = [
                        _PreparedRecord(
                            stream_name=self._rename_streams.get(
                                r.stream_name, r.stream_name
                            ),
                            message_id=r.message_id,
                            xadd_fields=self._prepare_fields(r),
                        )
                        for r in batch
                    ]
                    if first_ms is None:
                        first_ms = batch[0].message_id.ms
                    last_ms = max(last_ms, batch[-1].message_id.ms)
                    q.put((prepared, _BatchMeta(first_ms=first_ms, last_ms=last_ms)))
                    batch = []

            if batch and not self._stop_event.is_set():
                batch.sort(key=lambda r: r.message_id)
                prepared = [
                    _PreparedRecord(
                        stream_name=self._rename_streams.get(
                            r.stream_name, r.stream_name
                        ),
                        message_id=r.message_id,
                        xadd_fields=self._prepare_fields(r),
                    )
                    for r in batch
                ]
                if first_ms is None:
                    first_ms = batch[0].message_id.ms
                last_ms = max(last_ms, batch[-1].message_id.ms)
                q.put((prepared, _BatchMeta(first_ms=first_ms, last_ms=last_ms)))
        except (msgpack.UnpackValueError, OSError, KeyError, TypeError):
            logger.exception("Producer thread error")
        finally:
            q.put(None)

    def run(self) -> None:
        """Run the player, replaying all records from the file."""
        allowed_hosts = load_allowed_play_hosts()
        validate_play_target(self._conf.redis.host, allowed_hosts)

        self._running = True
        self._stop_event.clear()

        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._client = create_redis(self._conf.redis)
        self._client.ping()
        logger.info(
            f"Connected to Redis at {self._conf.redis.host}:{self._conf.redis.port}",
        )
        reader = RecordReader(self._conf.input)
        if self._exclude_streams:
            excluded = ", ".join(sorted(self._exclude_streams))
            logger.info(f"Excluding streams: {excluded}")
        if self._rename_streams:
            renames = ", ".join(f"{k} -> {v}" for k, v in self._rename_streams.items())
            logger.info(f"Renaming streams: {renames}")

        self._progress = tqdm(
            total=0,
            desc="Playing",
            bar_format="{desc}: {postfix} [{bar:30}] {percentage:3.0f}%",
            disable=not self._conf.verbose,
        )
        self._progress.set_postfix_str("00:00:00 / --:--:--")

        q: queue.Queue[tuple[list[_PreparedRecord], _BatchMeta] | None] = queue.Queue(
            maxsize=self._conf.prefetch,
        )
        producer = threading.Thread(
            target=self._producer_loop,
            args=(q, reader),
            daemon=True,
        )
        producer.start()

        prev_msg_id: MessageID | None = None
        prev_mono: float | None = None
        replayed = 0

        try:
            while self._running:
                try:
                    item = q.get(timeout=0.5)
                except queue.Empty:
                    continue

                if item is None:
                    break

                batch, meta = item
                self._replay_batch(batch, prev_msg_id, prev_mono)
                if batch:
                    prev_msg_id = batch[-1].message_id
                    prev_mono = time.monotonic()
                replayed += len(batch)

                elapsed_ms = batch[-1].message_id.ms - meta.first_ms if batch else 0
                total_ms = meta.last_ms - meta.first_ms
                self._progress.n = elapsed_ms
                self._progress.total = max(total_ms, 1)
                self._progress.set_postfix_str(
                    f"{_format_ms(elapsed_ms)} / {_format_ms(total_ms)}"
                )
                self._progress.refresh()

        finally:
            self._stop_event.set()
            producer.join(timeout=5.0)
            self._progress.close()
            self._progress = None
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)
            self._running = False

        logger.info(f"Playback complete: {replayed} messages replayed")

    def _flush_pipeline(
        self,
        pipe: redis_lib.client.Pipeline[bytes],
        pipe_meta: list[tuple[str, dict[str, str | bytes]]],
    ) -> None:
        """Execute a pipeline and log any per-command failures."""
        results = pipe.execute(raise_on_error=False)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                stream_name, xadd_fields = pipe_meta[i]
                field_keys = list(xadd_fields.keys())
                logger.error(
                    f"Failed to XADD to {stream_name} (fields: {field_keys}): {result}",
                )

    def _replay_batch(
        self,
        batch: list[_PreparedRecord],
        prev_msg_id: MessageID | None,
        prev_mono: float | None,
    ) -> None:
        """Replay a batch of prepared records with timing."""
        assert self._client is not None  # noqa: S101
        client = self._client
        speed = self._conf.speed
        max_delay = self._conf.max_delay

        logger.debug(f"Replaying batch of {len(batch)} records")

        local_prev_id = prev_msg_id
        local_prev_mono = prev_mono

        pipe = client.pipeline(transaction=False)
        pipe_count = 0
        pipe_meta: list[tuple[str, dict[str, str | bytes]]] = []

        for record in batch:
            if not self._running:
                break

            if local_prev_id is not None and local_prev_mono is not None:
                delta_ms = record.message_id.ms - local_prev_id.ms
                if delta_ms > 0:
                    delay = min((delta_ms / 1000.0) / speed, max_delay)
                    logger.debug(
                        f"Delay {delay:.3f}s (delta_ms={delta_ms}, speed={speed:.1f})",
                    )
                    if delay > 0:
                        # Flush pending commands before sleeping
                        if pipe_count > 0:
                            self._flush_pipeline(pipe, pipe_meta)
                            pipe = client.pipeline(transaction=False)
                            pipe_count = 0
                            pipe_meta = []

                        if self._stop_event.wait(timeout=delay):
                            break

            pipe.xadd(record.stream_name, record.xadd_fields, id="*")
            pipe_meta.append((record.stream_name, record.xadd_fields))
            pipe_count += 1

            local_prev_id = record.message_id
            local_prev_mono = time.monotonic()

        # Flush remaining commands
        if pipe_count > 0:
            self._flush_pipeline(pipe, pipe_meta)
